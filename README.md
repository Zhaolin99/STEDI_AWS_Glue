# STEDI_AWS_Glue
This project to builds a data lakehouse solution on AWS that curates data for training a machine learning model. This involves extracting data from sensors and the mobile app, transforming it, and storing it in AWS S3, ready for use by Data Scientists. Using Spark and AWS Glue to process data from the STEDI Step Trainer and mobile app. Main tasks include extracting data from sensors and the mobile app, curating it into a data lakehouse solution on AWS using Python and Spark with AWS Glue. This curated data will be utilized by Data Scientists to train the machine learning model. 


### Project Environment and Configuration
Utilize Python and Spark in AWS Glue to process data, AWS Athena to query it, and AWS S3 to store the curated data.Develop Python scripts using AWS Glue and Glue Studio. Save the code to a local Github Repository for version control. Testing and running Glue Jobs require submission to the AWS Glue environment.


Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

### Project Implementation:

1. 
Create your own S3 directories for customer_landing, step_trainer_landing, and accelerometer_landing zones, and copy the data there as a starting point.
 Create two Glue tables for the two landing zones. 
(Share your customer_landing.sql and your accelerometer_landing.sql script in git.)

2. 
Query those tables using Athena, and take a screenshot of each one showing the resulting data. Name the screenshots customer_landing(.png,.jpeg, etc.) and accelerometer_landing(.png,.jpeg, etc.).
Create 2 AWS Glue Jobs that do the following:
Sanitize the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called customer_trusted.
Sanitize the Accelerometer data from the Mobile App (Landing Zone) - and only store Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called accelerometer_trusted.

3.
You need to verify your Glue job is successful and only contains Customer Records from people who agreed to share their data. Query your Glue customer_trusted table with Athena and take a screenshot of the data. Name the screenshot customer_trusted(.png,.jpeg, etc.).
 Write a Glue job that does the following: (*The data from the Step Trainer Records has the correct serial numbers. Fulfuillment(landing zone) is incorrect)
Sanitize the Customer data (Trusted Zone) and create a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research called customers_curated.
Create two Glue Studio

4.
Read the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table called step_trainer_trusted that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).

5.
Create an aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data, and make a glue table called machine_learning_curated.
