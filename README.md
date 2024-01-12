# STEDI_AWS_Glue
This project to builds a data lakehouse solution on AWS that curates data for training a machine learning model. This involves extracting data from sensors and the mobile app, transforming it, and storing it in AWS S3, ready for use by Data Scientists. Using Spark and AWS Glue to process data from the STEDI Step Trainer and mobile app. Main tasks include extracting data from sensors and the mobile app, curating it into a data lakehouse solution on AWS using Python and Spark with AWS Glue. This curated data will be utilized by Data Scientists to train the machine learning model. 


## Project Environment and Configuration
Utilize Python and Spark in AWS Glue to process data, AWS Athena to query it, and AWS S3 to store the curated data.Develop Python scripts using AWS Glue and Glue Studio. Save the code to a local Github Repository for version control. Testing and running Glue Jobs require submission to the AWS Glue environment.

## Project Files
The Python code for Glue jobs has been executed and stored in the 'Scripts' folder, automatically generated from Glue Studio. The results of the Athena queries for customer landing and accelerometer landing have been recorded and saved in the "Screenshot" folder. The "SQL_scripts" folder contains the automatically generated SQL scripts for the Glue tables extracted from Athena.

## Project Implementation:

* Simulate data from different sources by creating S3 directories for landing zones. Two Glue tables, customer_landing and accelerometer_landing, will be created and their data queried using Athena, with screenshots saved in the GitHub repository.

* To maintain data integrity, two Glue Jobs will sanitize customer and accelerometer data, storing them in trusted zones as customer_trusted and accelerometer_trusted tables. Verification will be done by querying the customer_trusted table in Athena.

* Addressing a serial number bug in Customer Data, a Glue job will sanitize data in the Trusted Zone, creating a Glue Table in the Curated Zone named customers_curated.

* Two Glue Studio jobs read Step Trainer IoT data and create an aggregated table with associated accelerometer readings, storing them in the Trusted Zone Glue Table step_trainer_trusted and machine_learning_curated, respectively.
