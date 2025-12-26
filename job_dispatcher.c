#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <pthread.h>
#include <string.h>
#include <semaphore.h>

enum {PRIMES,PRIMEDIVISORS,ANAGRAMS,END};
int *occupied_servers = NULL;
sem_t sem;

typedef struct {
    int type;  
    int N;
    char name[9];
}server_request;

int factorial(int n){
    int result = 1;
    for(int i=2;i<=n;i++)
        result *= i;
    return result;
}

int is_prime(int n){
    for(int i=2;i*i <= n; i++)
        if(n % i == 0)
            return 0;
    
    return 1;
}

int primes(int n){
    int result = 0;
    for(int i=2; i<=n; i++){
        if(is_prime(i))
            result++;
    }
    return result;
}

int primedivisors(int n){
    int result=0;
    int a = 2;
    while(n > 1){
        if(n % a == 0){
            result++;
            n /= a;
        }
        else
            a++;
    }
    return result;
}

void swap(char *x, char *y){
    char temp = *x;
    *x = *y;
    *y = temp;
}

void permute(char *temp, char *name, int l, int r){
    if (l == r)
        sprintf(temp+strlen(temp),"%s",name);
    else{
        for(int i=l; i <= r; i++){
            swap((name + l),(name + i));
            permute(temp,name,l+1,r);
            swap((name + l),(name + i));
        }
    }
}

char *anagrams(char *name){
    int n = strlen(name);
    char *temp;
    temp = (char*)malloc(sizeof(char) * (factorial(n) + 1));
    if(temp == NULL){
        perror("memory allocation");
        exit(1);
    }
    temp[0] = '\0';
    permute(temp,name,0,n-1);

    return temp;
}

void *receive_results(void *arg){
    FILE *g = (FILE *)arg;
    int *result;
    while(1){
//        MPI_recv(result,1,MPI_INT,MPI_ANY_SOURCE,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
//        fprintf(g,"%d\n",*result);
    }

    return NULL;
}

void send_request(server_request request,int n, MPI_Datatype MPI_server_request){
    sem_wait(&sem);
    for(int i=1; i<n; i++){
        if(!occupied_servers[i]){
            MPI_Send(&request,1,MPI_server_request,i,0,MPI_COMM_WORLD);
            occupied_servers[i] = 0;
            sem_post(&sem);
            break;
        }
    }
}

void main_server(FILE *f, FILE *g, int comm_sz, MPI_Datatype MPI_server_request){
    char command[100];
    char *token,*request_type,*request_argument;
    int sleep_amount;        
    pthread_t th;
    server_request request;

    sem_init(&sem,0,comm_sz-1);

    occupied_servers = (int*)calloc(comm_sz,sizeof(int));
    if(occupied_servers == NULL){
        perror("memory allocation");
        exit(1);
    }

    if(pthread_create(th,NULL,receive_results,(void *)g) != 0){
        perror("creating thread");
        exit(1);
    }

    while(fgets(command,99,f) != NULL){
        token =  strtok(command," ");
        if(strcmp(token, "WAIT") == 0){
            token =  strtok(NULL," ");
            sleep_amount = atoi(token);
            sleep(sleep_amount);
        }
        else{
            request_type = strtok(NULL," ");
            request_argument = strtok(NULL, " ");
            if(strcmp(request_type,"PRIMES") == 0){
                request.type = PRIMES;
                request.N = atoi(request_argument);
                request.name[0] = '\0';
                send_request(request,comm_sz,MPI_server_request);
            }
            else if(strcmp(request_type,"PRIMEDIVISORS") == 0){
                request.type = PRIMEDIVISORS;
                request.N = atoi(request_argument);
                request.name[0] = '\0';
                send_request(request,comm_sz,MPI_server_request);
            }
            else if(strcmp(request_type,"ANAGRAMS") == 0){
                request.type = ANAGRAMS;    
                request.N = 0;
                strncpy(request.name,request_argument,9);
                send_request(request,comm_sz,MPI_server_request);
            }
            else{
                printf("Unknown command %s %s %s\n",token,request_type,request_argument);
                fflush(stdout);
            }
        }
    }

    sem_destroy(&sem);
    free(occupied_servers);
    pthread_join(th,NULL);
}

void worker_server(MPI_Datatype MPI_server_request){
    server_request request;
    int end = 0, result=0;
    char *anagrams_result = NULL;
    long int nr_bytes = 0;

    while(!end){
        MPI_Recv(&request,1,MPI_server_request,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);

        switch(request.type){
            case PRIMES:
                result = primes(request.N);
                break;
            case PRIMEDIVISORS:
                result = primedivisors(request.N);
                break;
            case ANAGRAMS:
                nr_bytes = factorial(strlen(request.name)) * strlen(request.name) + 1;
                anagrams_result = anagrams(request.name);
                break;
            case END:
                result = 0;
                end = 1;
                break;
        }
        if(request.type == ANAGRAMS){
            MPI_Send(&anagrams_result,nr_bytes,MPI_CHAR,0,ANAGRAMS,MPI_COMM_WORLD);
            free(anagrams_result);        
        }
        else
            MPI_Send(&result,1,MPI_INT,0,request.type,MPI_COMM_WORLD);

    }
}

int main(int argc, char **argv){
    int my_rank, comm_sz, block_counts[3];
    FILE *f,*g;
    MPI_Datatype MPI_server_request,old_types[3];
    MPI_Aint offsets[3];

    if(argc < 2){
        printf("No input file\n");
        exit(1);
    }

    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);

    offsets[0] = offsetof(server_request,type);
    old_types[0] = MPI_INT;
    block_counts[0] = 1;

    offsets[1] = offsetof(server_request,N);
    old_types[1] = MPI_INT;
    block_counts[1] = 1;

    offsets[2] = offsetof(server_request,name);
    old_types[2] = MPI_CHAR;
    block_counts[2] = 9;

    MPI_Type_create_struct(3,block_counts,offsets,old_types,&MPI_server_request);
    MPI_Type_commit(&MPI_server_request);

    if(my_rank == 0){
        if((f = fopen(argv[1],"r")) == NULL){
            printf("Problem opening input file\n");
            exit(1);
        }
        if((g = fopen("output.txt","w")) == NULL){
            printf("Problem opening output file\n");
            exit(1);
        }
        main_server(f,g,comm_sz,MPI_server_request);
        fclose(f);
        fclose(g);
    }
    else
        worker_server(MPI_server_request);

    MPI_Type_free(&MPI_server_request);
    MPI_Finalize();
    
    return 0;
}