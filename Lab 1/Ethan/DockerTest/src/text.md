What are docker image, container, and registry?
Docker image - Contains the instructions/files required to run an application
Docker container - virtualized runtime environment for applications
Docker registry - A service that hosts and distributes docker images e.g. Docker hub
List the Docker commands used in the video with a brief description for each command and option.
Dockerfile
From <base image>
RUN <command>
COPY <Src> <Destination> 
WOKRDIR <path>
CMD <command>
Docker
Docker build -t <tag> <path>
Docker run [-d] <image>
Docker ps [-a]
Docker logs <Container name/ID>
At the end of the video, there are two running containers, what commands can be used to stop and delete those two containers?
Docker stop <container name/ID>
Docker rm <Container name/ID>
What’s a multi-container Docker application?
An application consisting of multiple docker containers usually configured and managed with docker-compose 
How do these containers communicate together?
Using a bridge network
What command can be used to stop the Docker application and delete its images?
Docker-compose down –rmi all
List the new docker commands used in the video with a brief description for each command and option.
Docker run [-e VAR_NAME=VAR_VALUE] [--name Container_Name] <IMAGE>
Docker network create <network name>
Docker network connect <network name> <container name/ID>
Docker run [--network=NETWORK_NAME] <image> 	

