# spotify-airflow

This program extracts data from my Spotify Account using Spotify Web Apis and stores them in postgres database , specifically I extracted my listening history , I utilized Apache Airflow to create a DAG to schedule the my Data collection pipeline every day , After a few months I will end up with my own, private Spotify played tracks history dataset!

# Local Setup 

step 1. Download Docker 

step 3. ``` cd spotify-airflow ```

step 2. ``` docker-compose up -d  ```

step 3. ``` pip3 install -r requirements.txt ```

step 4. ```  python3 dags/spotify_dag.py ```

step 5.  Go to ``` localhost:8080 ``` to monitor pipeline 

# Future Work 

- create a visualisation dashboard 
