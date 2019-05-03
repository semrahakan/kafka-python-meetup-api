## ***Environment Setup***

Follow the instructions below to configure your local machine to run and develop this Python application which uses meetup stream data.

The instructions are focused on MacOsx.

#### Local Docker Environment

Install docker for mac

    https://hub.docker.com/editions/community/docker-ce-desktop-mac

Install docker compose 

    pip install docker-compose
    
Build docker instances

    docker build --no-cache .
    
#### Build Dependencies

Install python3

    brew install python3
    
Get virtual environment

    python3 -m venv meetup_kafka
   
#### Configure and Run

    source env/bin/activate
    
    (env) $ pip3 install -r requirements.txt
    
   To run the application
   
     (env) $ python producer.py
     (env) $ python consumer_writes_data.py
     (env) $ python consumer_group_events.py
     (env) $ python producer.py