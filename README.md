A server cluster with one main server that receives commands and dispatches jobs to the other servers and logs the time moment when each command has been received, when it has been dispatched to a worker server, and when it has been finished.  
The commands arriving at the server are simulated by a command  file.  A command file contains several lines, where each line may contain either a  request from a client to execute a specific command, or an indication that the server must wait a period of time before reading the next  request line. This wait is used to simulate a bursty arrival of client requests.  
The client requests can be:
-PRIMES N - find out how many primes there are in the first N natural numbers
-PRIMEDIVISORS N - find out how many prime divisors has the number N
-ANAGRAMS name - generates all anagrams (permutations) of name. It is ok to consider only names with up to 8 characters. 
