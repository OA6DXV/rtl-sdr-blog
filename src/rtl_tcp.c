/*
 * rtl-sdr, turns your Realtek RTL2832 based DVB dongle into a SDR receiver
 * Copyright (C) 2012 by Steve Markgraf <steve@steve-m.de>
 * Copyright (C) 2012-2013 by Hoernchen <la@tfc-server.de>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <errno.h>
#include <signal.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#ifndef _WIN32
#include <unistd.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <dirent.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netdb.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <limits.h>
#else
#include <winsock2.h>
#include <ws2tcpip.h>
#include "getopt/getopt.h"
#endif

#include <pthread.h>

#include "rtl-sdr.h"
#include "convenience/convenience.h"

#ifdef _WIN32
#pragma comment(lib, "ws2_32.lib")

typedef int socklen_t;

#else
#define closesocket close
#define SOCKADDR struct sockaddr
#define SOCKET int
#define SOCKET_ERROR -1
#endif

#define DEFAULT_PORT_STR "1234"
#define DEFAULT_SAMPLE_RATE_HZ 2048000
#define DEFAULT_MAX_NUM_BUFFERS 500
#define DATA_CLIENT_SEND_TIMEOUT_US 250000
#define DATA_CLIENT_SNDBUF_BYTES (1024 * 1024)

static SOCKET s;

static pthread_t tcp_worker_thread;
static pthread_t command_thread;
static pthread_cond_t exit_cond;
static pthread_mutex_t exit_cond_lock;

static pthread_mutex_t ll_mutex;
static pthread_cond_t cond;

typedef struct {
	SOCKET socket;
	char host[NI_MAXHOST];
	char port[NI_MAXSERV];
	char process[128];
} data_client_t;

static pthread_mutex_t clients_mutex;
static SOCKET listensocket_data = 0;
static data_client_t *data_clients = NULL;
static int data_client_count = 0;
static int data_client_capacity = 0;
static const char *data_port = NULL;
static int enable_data_port = 0;
static volatile int master_connected = 0;

struct llist {
	char *data;
	size_t len;
	struct llist *next;
};

typedef struct { /* structure size must be multiple of 2 bytes */
	char magic[4];
	uint32_t tuner_type;
	uint32_t tuner_gain_count;
} dongle_info_t;

static rtlsdr_dev_t *dev = NULL;
static dongle_info_t dongle_info;

static int enable_biastee = 0;
static int global_numq = 0;
static struct llist *ll_buffers = 0;
static int llbuf_num = DEFAULT_MAX_NUM_BUFFERS;

static volatile int do_exit = 0;
/* Set only by the OS signal handler (SIGINT/SIGTERM/...) to request a real
 * program shutdown. This is kept separate from do_exit, which is also used
 * internally to tear down a single client session. */
static volatile int shutdown_requested = 0;
/* -R: automatically recover when the device drops off the USB bus. */
static int enable_reconnect = 0;
/* Serial number and index of the device, used to find it again after a
 * disconnect/reconnect (the USB index may change after re-enumeration). */
static char dev_serial[256] = "";
static int dev_index_global = 0;


void usage(void)
{
	printf("rtl_tcp, an I/Q spectrum server for RTL2832 based DVB-T receivers\n\n");
	printf("Usage:\t[-a listen address]\n");
	printf("\t[-p listen port for master client with control commands (default: %s)]\n", DEFAULT_PORT_STR);
	printf("\t[-l listen port for read-only I/Q data clients (multiple clients allowed)]\n");
	printf("\t[-f frequency to tune to [Hz]]\n");
	printf("\t[-g gain (default: 0 for auto)]\n");
	printf("\t[-s samplerate in Hz (default: %d Hz)]\n", DEFAULT_SAMPLE_RATE_HZ);
	printf("\t[-b number of buffers (default: 15, set by library)]\n");
	printf("\t[-n max number of linked list buffers to keep (default: %d)]\n", DEFAULT_MAX_NUM_BUFFERS);
	printf("\t[-d device index (default: 0)]\n");
	printf("\t[-P ppm_error (default: 0)]\n");
	printf("\t[-T enable bias-T on GPIO PIN 0 (works for rtl-sdr.com v3/v4 dongles)]\n");
	printf("\t[-D enable direct sampling (default: off)]\n");
	printf("\t[-R automatically recover/reconnect if the device disconnects (e.g. power loss)]\n");
	printf("\nExample:\trtl_tcp -a 0.0.0.0 -p 1234 -l 1235\n");
	printf("\t\tPort 1234: Master client with full control (frequency, gain, ...)\n");
	printf("\t\tPort 1235: Read-only I/Q stream clients (multiple connections)\n");
	exit(1);
}

#ifdef _WIN32
int gettimeofday(struct timeval *tv, void* ignored)
{
	FILETIME ft;
	unsigned __int64 tmp = 0;
	if (NULL != tv) {
		GetSystemTimeAsFileTime(&ft);
		tmp |= ft.dwHighDateTime;
		tmp <<= 32;
		tmp |= ft.dwLowDateTime;
		tmp /= 10;
#ifdef _MSC_VER
		tmp -= 11644473600000000Ui64;
#else
		tmp -= 11644473600000000ULL;
#endif
		tv->tv_sec = (long)(tmp / 1000000UL);
		tv->tv_usec = (long)(tmp % 1000000UL);
	}
	return 0;
}

BOOL WINAPI
sighandler(int signum)
{
	if (CTRL_C_EVENT == signum) {
		fprintf(stderr, "Signal caught, exiting!\n");
		shutdown_requested = 1;
		do_exit = 1;
		rtlsdr_cancel_async(dev);
		return TRUE;
	}
	return FALSE;
}
#else
static void sighandler(int signum)
{
	signal(SIGPIPE, SIG_IGN);
	fprintf(stderr, "Signal caught, exiting!\n");
	shutdown_requested = 1;
	do_exit = 1;
	rtlsdr_cancel_async(dev);
}
#endif

/* Tear down the current client session without terminating the program.
 * Called by the worker threads when the client goes away or the data stream
 * stalls (which also happens when the device stops delivering samples). Unlike
 * the signal handler, this does not set shutdown_requested, so the main loop
 * can keep serving new clients (and, with -R, recover a lost device). */
static void stop_session(void)
{
#ifndef _WIN32
	signal(SIGPIPE, SIG_IGN);
#endif
	do_exit = 1;
	rtlsdr_cancel_async(dev);
}

static void refresh_dongle_info(void)
{
	int r;

	memset(&dongle_info, 0, sizeof(dongle_info));
	memcpy(&dongle_info.magic, "RTL0", 4);

	r = rtlsdr_get_tuner_type(dev);
	if (r >= 0)
		dongle_info.tuner_type = htonl(r);

	r = rtlsdr_get_tuner_gains(dev, NULL);
	if (r >= 0)
		dongle_info.tuner_gain_count = htonl(r);
}

static void set_socket_nonblock(SOCKET socket)
{
#ifdef _WIN32
	u_long blockmode = 1;
	ioctlsocket(socket, FIONBIO, &blockmode);
#else
	int r;

	r = fcntl(socket, F_GETFL, 0);
	if (r >= 0)
		fcntl(socket, F_SETFL, r | O_NONBLOCK);
#endif
}

#ifndef _WIN32
static int is_numeric_name(const char *name)
{
	while (*name) {
		if (!isdigit((unsigned char)*name))
			return 0;
		name++;
	}

	return 1;
}

static int find_peer_inode_ipv4(struct sockaddr_in *local,
				struct sockaddr_in *peer,
				unsigned long long *inode)
{
	FILE *fp;
	char line[512];
	char local_hex[64], remote_hex[64];
	unsigned int local_addr, remote_addr, local_port, remote_port, state;
	unsigned long long entry_inode;

	fp = fopen("/proc/net/tcp", "r");
	if (!fp)
		return -1;

	if (!fgets(line, sizeof(line), fp)) {
		fclose(fp);
		return -1;
	}

	while (fgets(line, sizeof(line), fp)) {
		if (sscanf(line, " %*d: %63[0-9A-Fa-f]:%x %63[0-9A-Fa-f]:%x %x %*s %*s %*s %*s %*u %*u %llu",
			   local_hex, &local_port, remote_hex, &remote_port,
			   &state, &entry_inode) != 6)
			continue;

		if (sscanf(local_hex, "%x", &local_addr) != 1 ||
		    sscanf(remote_hex, "%x", &remote_addr) != 1)
			continue;

		if (local_addr == peer->sin_addr.s_addr &&
		    local_port == ntohs(peer->sin_port) &&
		    remote_addr == local->sin_addr.s_addr &&
		    remote_port == ntohs(local->sin_port)) {
			*inode = entry_inode;
			fclose(fp);
			return 0;
		}
	}

	fclose(fp);
	return -1;
}

static int read_process_name(const char *pid, char *process, size_t process_len)
{
	FILE *fp;
	char path[PATH_MAX];
	char name[96];
	size_t len;

	snprintf(path, sizeof(path), "/proc/%s/comm", pid);
	fp = fopen(path, "r");
	if (!fp)
		return -1;

	if (!fgets(name, sizeof(name), fp)) {
		fclose(fp);
		return -1;
	}
	fclose(fp);

	len = strlen(name);
	if (len > 0 && name[len - 1] == '\n')
		name[len - 1] = '\0';

	snprintf(process, process_len, "%s/%s", pid, name);
	return 0;
}

static int find_process_by_socket_inode(unsigned long long inode,
					char *process, size_t process_len)
{
	DIR *proc_dir, *fd_dir;
	struct dirent *proc_ent, *fd_ent;
	char fd_dir_path[PATH_MAX], fd_path[PATH_MAX], link_target[128];
	char inode_target[64];
	ssize_t link_len;

	snprintf(inode_target, sizeof(inode_target), "socket:[%llu]", inode);

	proc_dir = opendir("/proc");
	if (!proc_dir)
		return -1;

	while ((proc_ent = readdir(proc_dir)) != NULL) {
		if (!is_numeric_name(proc_ent->d_name))
			continue;

		snprintf(fd_dir_path, sizeof(fd_dir_path), "/proc/%s/fd", proc_ent->d_name);
		fd_dir = opendir(fd_dir_path);
		if (!fd_dir)
			continue;

		while ((fd_ent = readdir(fd_dir)) != NULL) {
			snprintf(fd_path, sizeof(fd_path), "%s/%s", fd_dir_path, fd_ent->d_name);
			link_len = readlink(fd_path, link_target, sizeof(link_target) - 1);
			if (link_len < 0)
				continue;

			link_target[link_len] = '\0';
			if (!strcmp(link_target, inode_target)) {
				closedir(fd_dir);
				closedir(proc_dir);
				return read_process_name(proc_ent->d_name, process, process_len);
			}
		}

		closedir(fd_dir);
	}

	closedir(proc_dir);
	return -1;
}
#endif

static void get_local_peer_process(SOCKET socket, char *process, size_t process_len)
{
#ifndef _WIN32
	struct sockaddr_storage local, peer;
	socklen_t local_len = sizeof(local);
	socklen_t peer_len = sizeof(peer);
	unsigned long long inode;

	process[0] = '\0';

	if (getsockname(socket, (struct sockaddr *)&local, &local_len) ||
	    getpeername(socket, (struct sockaddr *)&peer, &peer_len))
		return;

	if (local.ss_family != AF_INET || peer.ss_family != AF_INET)
		return;

	if (find_peer_inode_ipv4((struct sockaddr_in *)&local,
				 (struct sockaddr_in *)&peer, &inode))
		return;

	find_process_by_socket_inode(inode, process, process_len);
#else
	process[0] = '\0';
#endif
}

static void add_data_client(SOCKET client, struct sockaddr *remote, socklen_t rlen)
{
	data_client_t *new_clients;
	int r, sndbuf;
	char host[NI_MAXHOST] = "unknown";
	char port[NI_MAXSERV] = "unknown";
	char process[128] = "";

	r = getnameinfo(remote, rlen, host, NI_MAXHOST, port, NI_MAXSERV,
			NI_NUMERICSERV | NI_NUMERICHOST);
	if (r)
		snprintf(host, sizeof(host), "unknown");

	sndbuf = DATA_CLIENT_SNDBUF_BYTES;
	setsockopt(client, SOL_SOCKET, SO_SNDBUF, (char *)&sndbuf, sizeof(sndbuf));
	get_local_peer_process(client, process, sizeof(process));
	set_socket_nonblock(client);

	pthread_mutex_lock(&clients_mutex);

	if (data_client_count >= data_client_capacity) {
		data_client_capacity = data_client_capacity == 0 ? 10 : data_client_capacity * 2;
		new_clients = realloc(data_clients, sizeof(data_client_t) * data_client_capacity);
		if (!new_clients) {
			data_client_capacity = data_client_count;
			pthread_mutex_unlock(&clients_mutex);
			closesocket(client);
			fprintf(stderr, "failed to allocate data client list\n");
			return;
		}
		data_clients = new_clients;
	}

	data_clients[data_client_count].socket = client;
	snprintf(data_clients[data_client_count].host,
		 sizeof(data_clients[data_client_count].host), "%s", host);
	snprintf(data_clients[data_client_count].port,
		 sizeof(data_clients[data_client_count].port), "%s", port);
	snprintf(data_clients[data_client_count].process,
		 sizeof(data_clients[data_client_count].process), "%s", process);
	data_client_count++;

	r = send(client, (const char *)&dongle_info, (int)sizeof(dongle_info), 0);
	if (sizeof(dongle_info) != r)
		printf("failed to send dongle information to data client %s:%s\n",
		       host, port);

	printf("data client accepted from %s:%s%s%s%s! total: %d\n",
	       host, port, process[0] ? " (" : "",
	       process[0] ? process : "", process[0] ? ")" : "",
	       data_client_count);
	pthread_mutex_unlock(&clients_mutex);
}

static void accept_data_clients(void)
{
	SOCKET client;
	struct sockaddr_storage remote;
	socklen_t rlen;

	if (!enable_data_port)
		return;

	while (!do_exit && !shutdown_requested) {
		rlen = sizeof(remote);
		client = accept(listensocket_data, (struct sockaddr *)&remote, &rlen);
		if (client == SOCKET_ERROR)
			break;
		add_data_client(client, (struct sockaddr *)&remote, rlen);
	}
}

static void send_data_to_clients(char *data, size_t len)
{
	fd_set writefds;
	struct timeval tv;
	size_t offset;
	int i, r, sent, new_count, removed;

	if (!enable_data_port || data_client_count <= 0)
		return;

	pthread_mutex_lock(&clients_mutex);

	for (i = 0; i < data_client_count; i++) {
		offset = 0;
		removed = 0;

		while (offset < len) {
			tv.tv_sec = 0;
			tv.tv_usec = DATA_CLIENT_SEND_TIMEOUT_US;

			FD_ZERO(&writefds);
			FD_SET(data_clients[i].socket, &writefds);
			r = select(data_clients[i].socket+1, NULL, &writefds, NULL, &tv);

			if (r <= 0) {
				break;
			}

			sent = send(data_clients[i].socket, &data[offset], (int)(len - offset), 0);
			if (sent <= 0) {
#ifdef _WIN32
				if (WSAGetLastError() == WSAEWOULDBLOCK)
					break;
#else
				if (errno == EAGAIN || errno == EWOULDBLOCK)
					break;
#endif
				printf("data client %s:%s%s%s%s socket bye\n",
				       data_clients[i].host, data_clients[i].port,
				       data_clients[i].process[0] ? " (" : "",
				       data_clients[i].process[0] ? data_clients[i].process : "",
				       data_clients[i].process[0] ? ")" : "");
				closesocket(data_clients[i].socket);
				data_clients[i].socket = SOCKET_ERROR;
				removed = 1;
				break;
			}

			offset += sent;
		}

		if (removed)
			printf("data clients remaining after %s:%s%s%s%s disconnected: %d\n",
			       data_clients[i].host, data_clients[i].port,
			       data_clients[i].process[0] ? " (" : "",
			       data_clients[i].process[0] ? data_clients[i].process : "",
			       data_clients[i].process[0] ? ")" : "",
			       data_client_count - 1);
	}

	new_count = 0;
	for (i = 0; i < data_client_count; i++) {
		if (data_clients[i].socket != SOCKET_ERROR)
			data_clients[new_count++] = data_clients[i];
	}
	data_client_count = new_count;

	pthread_mutex_unlock(&clients_mutex);
}

static int has_data_clients(void)
{
	int has_clients;

	if (!enable_data_port)
		return 0;

	pthread_mutex_lock(&clients_mutex);
	has_clients = data_client_count > 0;
	pthread_mutex_unlock(&clients_mutex);

	return has_clients;
}

static void send_data_to_master(char *data, size_t len)
{
	int bytesleft, bytessent, index;
	struct timeval tv = {1, 0};
	fd_set writefds;
	int r;

	if (!master_connected)
		return;

	bytesleft = len;
	index = 0;
	bytessent = 0;

	while(bytesleft > 0) {
		FD_ZERO(&writefds);
		FD_SET(s, &writefds);
		tv.tv_sec = 1;
		tv.tv_usec = 0;
		r = select(s+1, NULL, &writefds, NULL, &tv);
		if(r) {
			bytessent = send(s, &data[index], bytesleft, 0);
			if(bytessent <= 0) {
				printf("worker socket bye\n");
				master_connected = 0;
				if (!has_data_clients()) {
					stop_session();
					pthread_exit(NULL);
				}
				break;
			}
			bytesleft -= bytessent;
			index += bytessent;
		}
		if(bytessent == SOCKET_ERROR || do_exit) {
			printf("worker socket bye\n");
			master_connected = 0;
			if (!has_data_clients()) {
				stop_session();
				pthread_exit(NULL);
			}
			break;
		}
	}
}

void rtlsdr_callback(unsigned char *buf, uint32_t len, void *ctx)
{
	if(!do_exit) {
		struct llist *rpt = (struct llist*)malloc(sizeof(struct llist));
		rpt->data = (char*)malloc(len);
		memcpy(rpt->data, buf, len);
		rpt->len = len;
		rpt->next = NULL;

		pthread_mutex_lock(&ll_mutex);

		if (ll_buffers == NULL) {
			ll_buffers = rpt;
		} else {
			struct llist *cur = ll_buffers;
			int num_queued = 0;

			while (cur->next != NULL) {
				cur = cur->next;
				num_queued++;
			}

			if(llbuf_num && llbuf_num == num_queued-2){
				struct llist *curelem;

				free(ll_buffers->data);
				curelem = ll_buffers->next;
				free(ll_buffers);
				ll_buffers = curelem;
			}

			cur->next = rpt;

			if (num_queued > global_numq)
				printf("ll+, now %d\n", num_queued);
			else if (num_queued < global_numq)
				printf("ll-, now %d\n", num_queued);

			global_numq = num_queued;
		}
		pthread_cond_signal(&cond);
		pthread_mutex_unlock(&ll_mutex);
	}
}

static void *tcp_worker(void *arg)
{
	struct llist *curelem,*prev;
	struct timespec ts;
	struct timeval tp;
	int r = 0;

	while(1) {
		if(do_exit)
			pthread_exit(0);

		pthread_mutex_lock(&ll_mutex);
		gettimeofday(&tp, NULL);
		ts.tv_sec  = tp.tv_sec+5;
		ts.tv_nsec = tp.tv_usec * 1000;
		r = pthread_cond_timedwait(&cond, &ll_mutex, &ts);
		if(r == ETIMEDOUT) {
			pthread_mutex_unlock(&ll_mutex);
			printf("worker cond timeout\n");
			stop_session();
			pthread_exit(NULL);
		}

		curelem = ll_buffers;
		ll_buffers = 0;
		pthread_mutex_unlock(&ll_mutex);

		while(curelem != 0) {
			send_data_to_master(curelem->data, curelem->len);
			accept_data_clients();
			send_data_to_clients(curelem->data, curelem->len);
			prev = curelem;
			curelem = curelem->next;
			free(prev->data);
			free(prev);
		}
	}
}

static int set_gain_by_index(rtlsdr_dev_t *_dev, unsigned int index)
{
	int res = 0;
	int* gains;
	int count = rtlsdr_get_tuner_gains(_dev, NULL);

	if (count > 0 && (unsigned int)count > index) {
		gains = malloc(sizeof(int) * count);
		count = rtlsdr_get_tuner_gains(_dev, gains);

		res = rtlsdr_set_tuner_gain(_dev, gains[index]);

		free(gains);
	}

	return res;
}

#ifdef _WIN32
#define __attribute__(x)
#pragma pack(push, 1)
#endif
struct command{
	unsigned char cmd;
	unsigned int param;
}__attribute__((packed));
#ifdef _WIN32
#pragma pack(pop)
#endif
static void *command_worker(void *arg)
{
	int left, received = 0;
	fd_set readfds;
	struct command cmd={0, 0};
	struct timeval tv= {1, 0};
	int r = 0;
	uint32_t tmp;

	while(1) {
		left=sizeof(cmd);
		while(left >0) {
			FD_ZERO(&readfds);
			FD_SET(s, &readfds);
			tv.tv_sec = 1;
			tv.tv_usec = 0;
			r = select(s+1, &readfds, NULL, NULL, &tv);
			if(r) {
				received = recv(s, (char*)&cmd+(sizeof(cmd)-left), left, 0);
				if(received <= 0) {
					printf("comm recv bye\n");
					master_connected = 0;
					if (!has_data_clients())
						stop_session();
					pthread_exit(NULL);
				}
				left -= received;
			}
			if(received == SOCKET_ERROR || do_exit) {
				printf("comm recv bye\n");
				master_connected = 0;
				stop_session();
				pthread_exit(NULL);
			}
		}
		switch(cmd.cmd) {
		case 0x01:
			printf("set freq %d\n", ntohl(cmd.param));
			rtlsdr_set_center_freq(dev,ntohl(cmd.param));
			break;
		case 0x02:
			printf("set sample rate %d\n", ntohl(cmd.param));
			rtlsdr_set_sample_rate(dev, ntohl(cmd.param));
			break;
		case 0x03:
			printf("set gain mode %d\n", ntohl(cmd.param));
			rtlsdr_set_tuner_gain_mode(dev, ntohl(cmd.param));
			break;
		case 0x04:
			printf("set gain %d\n", ntohl(cmd.param));
			rtlsdr_set_tuner_gain(dev, ntohl(cmd.param));
			break;
		case 0x05:
			printf("set freq correction %d\n", ntohl(cmd.param));
			rtlsdr_set_freq_correction(dev, ntohl(cmd.param));
			break;
		case 0x06:
			tmp = ntohl(cmd.param);
			printf("set if stage %d gain %d\n", tmp >> 16, (short)(tmp & 0xffff));
			rtlsdr_set_tuner_if_gain(dev, tmp >> 16, (short)(tmp & 0xffff));
			break;
		case 0x07:
			printf("set test mode %d\n", ntohl(cmd.param));
			rtlsdr_set_testmode(dev, ntohl(cmd.param));
			break;
		case 0x08:
			printf("set agc mode %d\n", ntohl(cmd.param));
			rtlsdr_set_agc_mode(dev, ntohl(cmd.param));
			break;
		case 0x09:
			printf("set direct sampling %d\n", ntohl(cmd.param));
			rtlsdr_set_direct_sampling(dev, ntohl(cmd.param));
			break;
		case 0x0a:
			printf("set offset tuning %d\n", ntohl(cmd.param));
			rtlsdr_set_offset_tuning(dev, ntohl(cmd.param));
			break;
		case 0x0b:
			printf("set rtl xtal %d\n", ntohl(cmd.param));
			rtlsdr_set_xtal_freq(dev, ntohl(cmd.param), 0);
			break;
		case 0x0c:
			printf("set tuner xtal %d\n", ntohl(cmd.param));
			rtlsdr_set_xtal_freq(dev, 0, ntohl(cmd.param));
			break;
		case 0x0d:
			printf("set tuner gain by index %d\n", ntohl(cmd.param));
			set_gain_by_index(dev, ntohl(cmd.param));
			break;
		case 0x0e:
			printf("set bias tee %d\n", ntohl(cmd.param));
			rtlsdr_set_bias_tee(dev, (int)ntohl(cmd.param));
			break;
		default:
			break;
		}
		cmd.cmd = 0xff;
	}
}

/* Apply all device parameters. Used both at startup and when the device is
 * re-opened after a disconnect, so a recovered session keeps the same
 * configuration the user originally requested. */
static void configure_device(rtlsdr_dev_t *_dev, int direct_sampling,
			     int ppm_error, uint32_t samp_rate,
			     uint32_t frequency, int gain, int biastee)
{
	int r;

	/* Set direct sampling */
	if (direct_sampling)
		verbose_direct_sampling(_dev, 2);

	/* Set the tuner error */
	verbose_ppm_set(_dev, ppm_error);

	/* Set the sample rate */
	r = rtlsdr_set_sample_rate(_dev, samp_rate);
	if (r < 0)
		fprintf(stderr, "WARNING: Failed to set sample rate.\n");

	/* Set the frequency */
	r = rtlsdr_set_center_freq(_dev, frequency);
	if (r < 0)
		fprintf(stderr, "WARNING: Failed to set center freq.\n");
	else
		fprintf(stderr, "Tuned to %i Hz.\n", frequency);

	if (0 == gain) {
		/* Enable automatic gain */
		r = rtlsdr_set_tuner_gain_mode(_dev, 0);
		if (r < 0)
			fprintf(stderr, "WARNING: Failed to enable automatic gain.\n");
	} else {
		/* Enable manual gain */
		r = rtlsdr_set_tuner_gain_mode(_dev, 1);
		if (r < 0)
			fprintf(stderr, "WARNING: Failed to enable manual gain.\n");

		/* Set the tuner gain */
		r = rtlsdr_set_tuner_gain(_dev, gain);
		if (r < 0)
			fprintf(stderr, "WARNING: Failed to set tuner gain.\n");
		else
			fprintf(stderr, "Tuner gain set to %f dB.\n", gain/10.0);
	}

	rtlsdr_set_bias_tee(_dev, biastee);
	if (biastee)
		fprintf(stderr, "activated bias-T on GPIO PIN 0\n");

	/* Reset endpoint before we start reading from it (mandatory) */
	r = rtlsdr_reset_buffer(_dev);
	if (r < 0)
		fprintf(stderr, "WARNING: Failed to reset buffers.\n");
}

/* Report whether the open device still responds on the USB bus.
 * Re-applies the current center frequency, which performs real USB control /
 * I2C transfers and - crucially - returns a status we can check. We must NOT
 * use rtlsdr_get_usb_strings() here: it reads the cached device descriptor and
 * returns 0 even when the device is gone, producing a false "alive" result.
 * A stale handle (dongle unplugged, or re-enumerated at a new address) makes
 * these transfers fail with LIBUSB_ERROR_NO_DEVICE, which is exactly what we
 * want to detect. Re-tuning to the same frequency changes no radio setting. */
static int device_is_alive(void)
{
	if (!dev)
		return 0;

	return (rtlsdr_set_center_freq(dev, rtlsdr_get_center_freq(dev)) >= 0);
}

static void recovery_sleep(void)
{
#ifdef _WIN32
	Sleep(5000);
#else
	usleep(5000000);
#endif
}

/* Close the lost device and block until it (matched by serial number)
 * re-appears on the USB bus, then re-open and re-configure it. Returns 0 once
 * the device is back and ready, or -1 if a shutdown was requested while
 * waiting. */
static int reconnect_device(int direct_sampling, int ppm_error,
			    uint32_t samp_rate, uint32_t frequency,
			    int gain, int biastee)
{
	int idx, r;

	fprintf(stderr, "Device communication lost. Waiting for the device "
			"to reconnect...\n");

	if (dev) {
		rtlsdr_close(dev);
		dev = NULL;
	}

	while (!shutdown_requested) {
		if (dev_serial[0] != '\0') {
			idx = rtlsdr_get_index_by_serial(dev_serial);
		} else {
			/* No serial to match on: fall back to the original
			 * index if a device is present. */
			idx = (rtlsdr_get_device_count() > 0) ?
				dev_index_global : -1;
		}

		if (idx >= 0) {
			r = rtlsdr_open(&dev, (uint32_t)idx);
			if (0 == r && dev != NULL) {
				fprintf(stderr, "Device reconnected (index %d%s%s)."
					" Reinitializing...\n", idx,
					dev_serial[0] ? ", serial " : "",
					dev_serial[0] ? dev_serial : "");
				configure_device(dev, direct_sampling, ppm_error,
						 samp_rate, frequency, gain,
						 biastee);
				refresh_dongle_info();
				return 0;
			}
			if (dev) {
				rtlsdr_close(dev);
				dev = NULL;
			}
		}

		recovery_sleep();
	}

	return -1;
}

int main(int argc, char **argv)
{
	int r, opt, i;
	char *addr = "127.0.0.1";
	const char *port = DEFAULT_PORT_STR;
	uint32_t frequency = 100000000, samp_rate = DEFAULT_SAMPLE_RATE_HZ;
	struct sockaddr_storage local, remote;
	struct addrinfo *ai;
	struct addrinfo *aiHead;
	struct addrinfo  hints = { 0 };
	struct addrinfo *aiData;
	struct addrinfo *aiDataHead = NULL;
	struct addrinfo  hintsData = { 0 };
	char hostinfo[NI_MAXHOST];
	char portinfo[NI_MAXSERV];
	char remhostinfo[NI_MAXHOST];
	char remportinfo[NI_MAXSERV];
	char remprocessinfo[128];
	int aiErr;
	int client_index;
	uint32_t buf_num = 0;
	int dev_index = 0;
	int dev_given = 0;
	int gain = 0;
	int ppm_error = 0;
	int direct_sampling = 0;
	struct llist *curelem,*prev;
	pthread_attr_t attr;
	void *status;
	struct timeval tv = {1,0};
	struct linger ling = {1,0};
	SOCKET listensocket = 0;
	socklen_t rlen;
	fd_set readfds;
	u_long blockmode = 1;
#ifdef _WIN32
	WSADATA wsd;
	i = WSAStartup(MAKEWORD(2,2), &wsd);
#else
	struct sigaction sigact, sigign;
#endif

	/* Line-buffer stdout so log messages appear in real time under systemd
	 * (otherwise printf() output is block-buffered and only flushed in bursts,
	 * e.g. when the service stops, which makes the logs very hard to read). */
	setvbuf(stdout, NULL, _IOLBF, 0);

	while ((opt = getopt(argc, argv, "a:p:l:f:g:s:b:n:d:P:TDR")) != -1) {
		switch (opt) {
		case 'd':
			dev_index = verbose_device_search(optarg);
			dev_given = 1;
			break;
		case 'f':
			frequency = (uint32_t)atofs(optarg);
			break;
		case 'g':
			gain = (int)(atof(optarg) * 10); /* tenths of a dB */
			break;
		case 's':
			samp_rate = (uint32_t)atofs(optarg);
			break;
		case 'a':
		        addr = strdup(optarg);
			break;
		case 'p':
		        port = strdup(optarg);
			break;
		case 'l':
		        data_port = strdup(optarg);
			enable_data_port = 1;
			break;
		case 'b':
			buf_num = atoi(optarg);
			break;
		case 'n':
			llbuf_num = atoi(optarg);
			break;
		case 'P':
			ppm_error = atoi(optarg);
			break;
		case 'T':
			enable_biastee = 1;
			break;
		case 'D':
			direct_sampling = 1;
			break;
		case 'R':
			enable_reconnect = 1;
			break;
		default:
			usage();
			break;
		}
	}

	if (argc < optind)
		usage();

	if (!dev_given) {
		dev_index = verbose_device_search("0");
	}

	if (dev_index < 0) {
	    exit(1);
	}

	rtlsdr_open(&dev, (uint32_t)dev_index);
	if (NULL == dev) {
	fprintf(stderr, "Failed to open rtlsdr device #%d.\n", dev_index);
		exit(1);
	}

	/* Remember how to find this exact device again after a reconnect.
	 * The serial survives re-enumeration (the USB index may not). */
	dev_index_global = dev_index;
	if (rtlsdr_get_usb_strings(dev, NULL, NULL, dev_serial) < 0)
		dev_serial[0] = '\0';

#ifndef _WIN32
	sigact.sa_handler = sighandler;
	sigemptyset(&sigact.sa_mask);
	sigact.sa_flags = 0;
	sigign.sa_handler = SIG_IGN;
	sigaction(SIGINT, &sigact, NULL);
	sigaction(SIGTERM, &sigact, NULL);
	sigaction(SIGQUIT, &sigact, NULL);
	sigaction(SIGPIPE, &sigign, NULL);
#else
	SetConsoleCtrlHandler( (PHANDLER_ROUTINE) sighandler, TRUE );
#endif

	configure_device(dev, direct_sampling, ppm_error, samp_rate, frequency,
			 gain, enable_biastee);
	refresh_dongle_info();

	pthread_mutex_init(&exit_cond_lock, NULL);
	pthread_mutex_init(&ll_mutex, NULL);
	pthread_mutex_init(&clients_mutex, NULL);
	pthread_cond_init(&cond, NULL);
	pthread_cond_init(&exit_cond, NULL);

	hints.ai_flags  = AI_PASSIVE; /* Server mode. */
	hints.ai_family = PF_UNSPEC;  /* IPv4 or IPv6. */
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;

	if ((aiErr = getaddrinfo(addr,
				 port,
				 &hints,
				 &aiHead )) != 0)
	{
		fprintf(stderr, "local address %s ERROR - %s.\n",
		        addr, gai_strerror(aiErr));
		return(-1);
	}
	memcpy(&local, aiHead->ai_addr, aiHead->ai_addrlen);

	for (ai = aiHead; ai != NULL; ai = ai->ai_next) {
		aiErr = getnameinfo((struct sockaddr *)ai->ai_addr, ai->ai_addrlen,
				    hostinfo, NI_MAXHOST,
				    portinfo, NI_MAXSERV, NI_NUMERICSERV | NI_NUMERICHOST);
		if (aiErr)
			fprintf( stderr, "getnameinfo ERROR - %s.\n",hostinfo);

		listensocket = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
		if (listensocket < 0)
			continue;

		r = 1;
		setsockopt(listensocket, SOL_SOCKET, SO_REUSEADDR, (char *)&r, sizeof(int));
		setsockopt(listensocket, SOL_SOCKET, SO_LINGER, (char *)&ling, sizeof(ling));

		if (bind(listensocket, (struct sockaddr *)&local, aiHead->ai_addrlen))
			fprintf(stderr, "rtl_tcp bind error: %s", strerror(errno));
		else
			break;
	}

#ifdef _WIN32
	ioctlsocket(listensocket, FIONBIO, &blockmode);
#else
	r = fcntl(listensocket, F_GETFL, 0);
	r = fcntl(listensocket, F_SETFL, r | O_NONBLOCK);
#endif

	if (enable_data_port) {
		hintsData.ai_flags = AI_PASSIVE;
		hintsData.ai_family = PF_UNSPEC;
		hintsData.ai_socktype = SOCK_STREAM;
		hintsData.ai_protocol = IPPROTO_TCP;

		if ((aiErr = getaddrinfo(addr,
					 data_port,
					 &hintsData,
					 &aiDataHead )) != 0)
		{
			fprintf(stderr, "data address %s ERROR - %s.\n",
				addr, gai_strerror(aiErr));
			return(-1);
		}

		for (aiData = aiDataHead; aiData != NULL; aiData = aiData->ai_next) {
			listensocket_data = socket(aiData->ai_family,
						   aiData->ai_socktype,
						   aiData->ai_protocol);
			if (listensocket_data < 0)
				continue;

			r = 1;
			setsockopt(listensocket_data, SOL_SOCKET, SO_REUSEADDR, (char *)&r, sizeof(int));
			setsockopt(listensocket_data, SOL_SOCKET, SO_LINGER, (char *)&ling, sizeof(ling));

			if (bind(listensocket_data, aiData->ai_addr, aiData->ai_addrlen))
				fprintf(stderr, "rtl_tcp data bind error: %s", strerror(errno));
			else
				break;

			closesocket(listensocket_data);
			listensocket_data = 0;
		}

		if (!listensocket_data) {
			fprintf(stderr, "failed to bind data port %s\n", data_port);
			return(-1);
		}

		set_socket_nonblock(listensocket_data);
		listen(listensocket_data, 10);
		freeaddrinfo(aiDataHead);
	}

	while(1) {
		printf("listening...\n");
		printf("Use the device argument 'rtl_tcp=%s:%s' in OsmoSDR "
		       "(gr-osmosdr) source\n"
		       "to receive samples in GRC and control "
		       "rtl_tcp parameters (frequency, gain, ...).\n",
		       hostinfo, portinfo);
		if (enable_data_port)
			printf("Data port for read-only clients: %s\n", data_port);
		listen(listensocket,1);

		while(1) {
			FD_ZERO(&readfds);
			FD_SET(listensocket, &readfds);
			if (enable_data_port)
				FD_SET(listensocket_data, &readfds);
			tv.tv_sec = 1;
			tv.tv_usec = 0;
			r = select((listensocket_data > listensocket ? listensocket_data : listensocket)+1,
				   &readfds, NULL, NULL, &tv);
			if(shutdown_requested) {
				goto out;
			}

			if (enable_reconnect && !shutdown_requested && dev && !device_is_alive()) {
				if (reconnect_device(direct_sampling, ppm_error, samp_rate, frequency, gain, enable_biastee) < 0)
					goto out;
			}

			else if(r) {
				if (enable_data_port && FD_ISSET(listensocket_data, &readfds))
					accept_data_clients();

				if (!FD_ISSET(listensocket, &readfds))
					continue;

				rlen = sizeof(remote);
				s = accept(listensocket,(struct sockaddr *)&remote, &rlen);
				master_connected = 1;
				break;
			}
		}

		setsockopt(s, SOL_SOCKET, SO_LINGER, (char *)&ling, sizeof(ling));

		getnameinfo((struct sockaddr *)&remote, rlen,
			    remhostinfo, NI_MAXHOST,
			    remportinfo, NI_MAXSERV, NI_NUMERICSERV);
		get_local_peer_process(s, remprocessinfo, sizeof(remprocessinfo));
		printf("client accepted! %s %s%s%s%s\n",
		       remhostinfo, remportinfo,
		       remprocessinfo[0] ? " (" : "",
		       remprocessinfo[0] ? remprocessinfo : "",
		       remprocessinfo[0] ? ")" : "");

		refresh_dongle_info();

		r = send(s, (const char *)&dongle_info, sizeof(dongle_info), 0);
		if (sizeof(dongle_info) != r)
			printf("failed to send dongle information\n");

		pthread_attr_init(&attr);
		pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
		r = pthread_create(&tcp_worker_thread, &attr, tcp_worker, NULL);
		r = pthread_create(&command_thread, &attr, command_worker, NULL);
		pthread_attr_destroy(&attr);

		r = rtlsdr_read_async(dev, rtlsdr_callback, NULL, buf_num, 0);

		pthread_join(tcp_worker_thread, &status);
		pthread_join(command_thread, &status);

		closesocket(s);
		master_connected = 0;

		printf("all threads dead..\n");
		curelem = ll_buffers;
		ll_buffers = 0;

		while(curelem != 0) {
			prev = curelem;
			curelem = curelem->next;
			free(prev->data);
			free(prev);
		}

		do_exit = 0;
		global_numq = 0;

		/* The session may have ended because the device dropped off the
		 * USB bus (e.g. power loss to a self-powered hub). Without this
		 * the loop would spin forever re-running rtlsdr_read_async() on a
		 * dead handle (the "zombie" state). With -R, detect the loss and
		 * wait for the dongle to come back, then re-open and continue. */
		if (enable_reconnect && !shutdown_requested && !device_is_alive()) {
			if (reconnect_device(direct_sampling, ppm_error, samp_rate,
					     frequency, gain, enable_biastee) < 0)
				goto out;
		}
	}

out:
	if (dev)
		rtlsdr_close(dev);
	closesocket(listensocket);
	closesocket(s);
	if (enable_data_port) {
		closesocket(listensocket_data);
		for (client_index = 0; client_index < data_client_count; client_index++)
			closesocket(data_clients[client_index].socket);
		free(data_clients);
	}
#ifdef _WIN32
	WSACleanup();
#endif
	printf("bye!\n");
	return r >= 0 ? r : -r;
}
