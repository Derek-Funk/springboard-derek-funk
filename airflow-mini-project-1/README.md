# Airflow Mini-Project 1
This project sets up an Airflow workflow that emails you the daily 1-minute interval stock price data for Apple and Tesla, every weekday at 6pm.

# Workflow Set Up
* download docker and docker-compose
* start docker engine
* download this repository
* if you would like Airflow to send you emails, create a new gmail account with the *Less secure app access* feature ON
* in *airflow/airflow.cfg*, change:
    * *smtp_user* and *smtp_mail_from* to the full email address
    * *smtp_password* to the email password
* in the repository, build the containers: $ docker-compose up -d --build
* view status of containers: $ docker ps
* see contents of a container: $ docker exec -it <CONTAINER ID> bash
* open the web scheduler in a browser at http://localhost:8080/
* find the DAG *marketvol* and toggle it ON
* You should now receive an email every weekday at 6pm with Apple and Tesla stock price data as attachments. It will look like this: ![This is a alt text.](/images/image_emailAttachments.png "image_emailAttachments.png")
* you can shutdown the app at anytime: $ docker-compose down

# Manual DAG Run
* you can manually trigger the DAG in the web scheduler UI
* a successful run looks like this: ![This is a alt text.](/images/image_successfulRun.png "image_successfulRun.png")
* If it is a weekend day, there will be no stock file. In this case, in *airflow/dags/marketvol.py* you can temporarily change *start_date* to a weekday, then rebuild the containers and trigger the DAG.

# Sources
* https://towardsdatascience.com/deploy-apache-airflow-in-multiple-docker-containers-7f17b8b3de58