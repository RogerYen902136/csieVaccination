/**
 * Programming Assignment 1 - csieVaccination
 * System Programming, Fall 2021, National Taiwan University
 *
 * @author	rogeryen
 */

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define ERR_EXIT(a) do { perror(a); exit(EXIT_FAILURE); } while (0)

typedef struct {
	int id;		/* 902001 - 902020 */
	int AZ;
	int BNT;
	int Moderna;
} registerRecord;

typedef struct {
	char 		hostname[512];	/* server's hostname */
	unsigned short	port;		/* port to listen */
	int 		listen_fd;	/* fd to wait for a new connection */
} server;

typedef struct {
	char 		host[512];  	/* client's host */
	int 		conn_fd;  	/* fd to talk with client */
	char 		buf[512];  	/* data sent by/to client */
	size_t 		buf_len; 	/* bytes used by buf */
	int 		id;
	int 		wait_for_write; /* used by handle_read to know if the header is read or not */
	registerRecord 	record;
} request;

server	svr;				/* server */
request *requestP = NULL;	/* point to a list of requests */
int 	maxfd;				/* size of open file descriptor table, size of request list */

/* initialize a server, exit for error */
static void init_server(unsigned short port);

/* initialize a request instance */
static void init_request(request *reqP);

/* free resources used by a request instance */
static void free_request(request *reqP);

/* handle input from a client request */
int handle_read(request *reqP);

/* send message to the file with the given file descriptor */
void send_msg(int fd, const char *msg);

/* close the connection from client, and clear the corresponding bit in fd_set */
void disconnect(request *reqP, fd_set *setP);

/* check whether user id of a request is valid */
int id_is_valid(request *reqP);

/* store the preference order of a request in buf, with the specified format */
void get_order(request *reqP, char *buf);

/* store the new preference order in buf, with the specified format, retVal indicates whether input order is valid */
int modify_order(request *reqP, char *buf);

int
main(int argc, char *argv[]) {
	/* Parse args. */
	if (argc != 2) {
		fprintf(stderr, "usage: %s [port]\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	struct sockaddr_in 	cliaddr;	/* used by accept() */
	socklen_t 	  	clilen;

	int 	conn_fd;	/* fd for a new connection with client */
	int 	file_fd;  	/* fd for file that we open for reading */
	char 	buf[512];

	fd_set	mset;	/* master fd_set */
	fd_set  wset;	/* working fd_set */

	struct flock 	lock;		/* used for file locking */
	off_t 		currpos;	/* used for lseek() and file locking */
	int 		wr_locked[20] = {0};

	/* Initialize server. */
	init_server((unsigned short) strtol(argv[1], NULL, 10));

	/* Initialize master fd_set. */
	FD_ZERO(&mset);
	FD_SET(svr.listen_fd, &mset);

	/* Open registerRecord file. */
	if ((file_fd = open("registerRecord", O_RDWR)) < 0) {
		ERR_EXIT("open");
	}

	/* Loop for handling connections. */
	fprintf(stderr, "\nstarting on %.80s, port %d, fd %d, maxconn %d...\n",
			svr.hostname, svr.port, svr.listen_fd, maxfd);

	while (1) {
		/* Apply I/O multiplexing using select(). */
		memcpy(&wset, &mset, sizeof(mset));
		if (select(maxfd, &wset, NULL, NULL, NULL) < 0) {
			ERR_EXIT("select");
		}

		for (int i = 0; i < maxfd; i++) {
			if (!FD_ISSET(i, &wset)) {
				continue;
			}

			if (i == svr.listen_fd) {
				/* Check new connection. */
				clilen = sizeof(cliaddr);
				if ((conn_fd = accept(svr.listen_fd, (struct sockaddr *) &cliaddr, &clilen)) < 0) {
					if (errno == EINTR || errno == EAGAIN) {
						continue;	/* try again */
					}
					if (errno == ENFILE) {
						fprintf(stderr, "out of file descriptor table ... (maxconn %d)\n", maxfd);
						continue;
					}
					ERR_EXIT("accept");
				}

				requestP[conn_fd].conn_fd = conn_fd;
				strcpy(requestP[conn_fd].host, inet_ntoa(cliaddr.sin_addr));
				fprintf(stderr, "getting a new request... fd %d from %s\n", conn_fd, requestP[conn_fd].host);

				FD_SET(conn_fd, &mset);
				send_msg(conn_fd, "Please enter your id (to check your preference order):\n");

			} else {
				int ret = handle_read(&requestP[i]);	/* Parse data from client to requestP[i].buf. */
				if (ret < 0) {
					fprintf(stderr, "bad request from %s\n", requestP[conn_fd].host);
					disconnect(&requestP[i], &mset);
					continue;
				}

				/* Handle requests from clients. */
#ifdef READ_SERVER
				/* Check whether input id is valid. */
				if (!id_is_valid(&requestP[i])) {
					send_msg(requestP[i].conn_fd, "[Error] Operation failed. Please try again.\n");
					disconnect(&requestP[i], &mset);
					break;
				}
				currpos = (requestP[i].id - 902001) * (off_t) sizeof(registerRecord);

				/* Acquire read lock and answer request. */
				lock.l_type = F_RDLCK;
				lock.l_whence = SEEK_SET;
				lock.l_start = currpos;
				lock.l_len = sizeof(registerRecord);

				if (fcntl(file_fd, F_SETLK, &lock) < 0) {
					if (errno != EAGAIN) {
						ERR_EXIT("fcntl");
					}
					send_msg(requestP[i].conn_fd, "Locked.\n");

				} else {
					if (lseek(file_fd, currpos, SEEK_SET) < 0) {
						ERR_EXIT("lseek");
					}
					if (read(file_fd, &requestP[i].record, sizeof(registerRecord)) != sizeof(registerRecord)) {
						ERR_EXIT("read");
					}

					get_order(&requestP[i], buf);
					send_msg(requestP[i].conn_fd, buf);

					/* Release read lock. */
					lock.l_type = F_UNLCK;
					lock.l_whence = SEEK_SET;
					lock.l_start = currpos;
					lock.l_len = sizeof(registerRecord);

					if (fcntl(file_fd, F_SETLK, &lock) < 0) {
						ERR_EXIT("fcntl");
					}
				}
				disconnect(&requestP[i], &mset);
				break;

#elif defined WRITE_SERVER
				if (requestP[i].wait_for_write == 0) {
					/* User id hasn't been obtained. */
					/* Check whether input id is valid. */
					if (!id_is_valid(&requestP[i])) {
						send_msg(requestP[i].conn_fd, "[Error] Operation failed. Please try again.\n");
						disconnect(&requestP[i], &mset);
						break;
					}
					currpos = (requestP[i].id - 902001) * (off_t) sizeof(registerRecord);

					/* Acquire write lock and answer the 1st half of request. */
					lock.l_type = F_WRLCK;
					lock.l_whence = SEEK_SET;
					lock.l_start = currpos;
					lock.l_len = sizeof(registerRecord);

					if (fcntl(file_fd, F_SETLK, &lock) < 0) {
						if (errno != EAGAIN) {
							ERR_EXIT("fcntl");
						}
						send_msg(requestP[i].conn_fd, "Locked.\n");
						disconnect(&requestP[i], &mset);
						break;
					}
					if (wr_locked[requestP[i].id - 902001] == 1) {
						send_msg(requestP[i].conn_fd, "Locked.\n");
						disconnect(&requestP[i], &mset);
						break;
					}

					wr_locked[requestP[i].id - 902001] = 1;
					requestP[i].wait_for_write = 1;

					if (lseek(file_fd, currpos, SEEK_SET) < 0) {
						ERR_EXIT("lseek");
					}
					if (read(file_fd, &requestP[i].record, sizeof(registerRecord)) != sizeof(registerRecord)) {
						ERR_EXIT("read");
					}

					get_order(&requestP[i], buf);
					send_msg(requestP[i].conn_fd, buf);
					send_msg(requestP[i].conn_fd, "Please input your preference order respectively(AZ,BNT,Moderna):\n");
					break;

				} else {
					currpos = (requestP[i].id - 902001) * (off_t) sizeof(registerRecord);

					/* Answer the 2nd half of request, which is to update the preference order. */
					if (!modify_order(&requestP[i], buf)) {
						send_msg(requestP[i].conn_fd, "[Error] Operation failed. Please try again.\n");
					} else {
						if (lseek(file_fd, currpos, SEEK_SET) < 0) {
							ERR_EXIT("lseek");
						}
						/* Update registerRecord. */
						if (write(file_fd, &requestP[i].record, sizeof(registerRecord)) != sizeof(registerRecord)) {
							ERR_EXIT("write");
						}
						/* Inform client the update has been done. */
						send_msg(requestP[i].conn_fd, buf);
					}

					/* Release write lock. */
					wr_locked[requestP[i].id - 902001] = 0;
					lock.l_type = F_UNLCK;
					lock.l_whence = SEEK_SET;
					lock.l_start = currpos;
					lock.l_len = sizeof(registerRecord);

					if (fcntl(file_fd, F_SETLK, &lock) < 0) {
						ERR_EXIT("fcntl");
					}
					disconnect(&requestP[i], &mset);
					break;
				}
#endif
			}
		}
	}

	close(file_fd);
	free(requestP);
	exit(EXIT_SUCCESS);
}

static void
init_server(unsigned short port) {
	struct sockaddr_in 	servaddr;
	int 			tmp;

	gethostname(svr.hostname, sizeof(svr.hostname));
	svr.port = port;

	if ((svr.listen_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
		ERR_EXIT("socket");
	}

	bzero(&servaddr, sizeof(servaddr));
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
	servaddr.sin_port = htons(port);
	tmp = 1;

	if (setsockopt(svr.listen_fd, SOL_SOCKET, SO_REUSEADDR, (void *) &tmp, sizeof(tmp)) < 0) {
		ERR_EXIT("setsockopt");
	}
	if (bind(svr.listen_fd, (struct sockaddr *) &servaddr, sizeof(servaddr)) < 0) {
		ERR_EXIT("bind");
	}
	if (listen(svr.listen_fd, 1024) < 0) {
		ERR_EXIT("listen");
	}

	// Get file descriptor table size and initialize request table
	maxfd = getdtablesize();
	requestP = (request *) malloc(sizeof(request) * maxfd);
	if (requestP == NULL) {
		ERR_EXIT("out of memory allocating all requests");
	}
	for (int i = 0; i < maxfd; i++) {
		init_request(&requestP[i]);
	}
	requestP[svr.listen_fd].conn_fd = svr.listen_fd;
	strcpy(requestP[svr.listen_fd].host, svr.hostname);
}

static void
init_request(request *reqP) {
	reqP->conn_fd = -1;
	reqP->buf_len = 0;
	reqP->id = 0;
	reqP->wait_for_write = 0;
	memset(&reqP->record, 0, sizeof(registerRecord));
}

static void
free_request(request *reqP) {
	init_request(reqP);
}

int
handle_read(request *reqP) {
	ssize_t r;
	char 	buf[512];
	char 	*p1;
	size_t 	len;

	if ((r = read(reqP->conn_fd, buf, sizeof(buf))) <= 0) {
		return r == 0 ? 0 : -1;
	}
	if ((p1 = strstr(buf, "\015\012")) == NULL) {
		if ((p1 = strstr(buf, "\012")) == NULL) {
			ERR_EXIT("this really should not happen...");
		}
	}

	len = p1 - buf + 1;
	memmove(reqP->buf, buf, len);
	reqP->buf[len - 1] = '\0';
	reqP->buf_len = len - 1;
	return 1;
}

void
send_msg(int fd, const char *msg) {
	char buf[512];

	sprintf(buf, "%s", msg);
	if (write(fd, buf, strlen(buf)) != strlen(buf)) {
		ERR_EXIT("write");
	}
}

void
disconnect(request *reqP, fd_set *setP) {
	FD_CLR(reqP->conn_fd, setP);
	close(reqP->conn_fd);
	free_request(reqP);
}

int
id_is_valid(request *reqP) {
	if (reqP->buf_len == 6) {
		reqP->id = (int) strtol(reqP->buf, NULL, 10);
		if (902001 <= reqP->id && reqP->id <= 902020) {
			return 1;
		}
	}
	return 0;
}

void
get_order(request *reqP, char *buf) {
	sprintf(buf, "Your preference order is %s > %s > %s.\n",
			reqP->record.AZ == 1 ? "AZ" : reqP->record.BNT == 1 ? "BNT" : "Moderna",
			reqP->record.AZ == 2 ? "AZ" : reqP->record.BNT == 2 ? "BNT" : "Moderna",
			reqP->record.AZ == 3 ? "AZ" : reqP->record.BNT == 3 ? "BNT" : "Moderna"
	);
}

int arr[6][3] = {{1, 2, 3},
				 {1, 3, 2},
				 {2, 1, 3},
				 {2, 3, 1},
				 {3, 1, 2},
				 {3, 2, 1}};

int
modify_order(request *reqP, char *buf) {
	if (reqP->buf_len == 5) {
		char s1[512];
		char s2[512];
		char s3[512];

		if (sscanf(reqP->buf, "%s%s%s", s1, s2, s3) == 3) {
			if (strlen(s1) == 1 && strlen(s2) == 1 && strlen(s3) == 1) {
				if (isdigit(s1[0]) && isdigit(s2[0]) && isdigit(s3[0])) {
					int	n1 = s1[0] - '0';
					int n2 = s2[0] - '0';
					int n3 = s3[0] - '0';

					int fl = 0;
					for (int i = 0; i < 6; i++) {
						if (n1 == arr[i][0] && n2 == arr[i][1] && n3 == arr[i][2]) {
							fl = 1;
							break;
						}
					}
					if (fl) {
						reqP->record.AZ = n1;
						reqP->record.BNT = n2;
						reqP->record.Moderna = n3;

						sprintf(buf, "Preference order for %d modified successed, new preference order is %s > %s > %s.\n",
								reqP->id,
								reqP->record.AZ == 1 ? "AZ" : reqP->record.BNT == 1 ? "BNT" : "Moderna",
								reqP->record.AZ == 2 ? "AZ" : reqP->record.BNT == 2 ? "BNT" : "Moderna",
								reqP->record.AZ == 3 ? "AZ" : reqP->record.BNT == 3 ? "BNT" : "Moderna"
						);
						return 1;
					}
				}
			}
		}
	}

	return 0;
}
