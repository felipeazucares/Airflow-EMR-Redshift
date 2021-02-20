
Readme.md
---------

Introduction
----

This Git Repo is my submission to the data engineering Nano degree capstone project. It should contain all the files and instructions necessary to set up and install the project on another machine.

Purpose
--
The purpose of the project is to create a star schema in AWS Redshift which allows users to correlate the visa types (business, pleasure, study) and/or gender mix of arrivals to each US state by month with the temperature conditions and other demographic details for that state. One possible purpose of this would be to gain insight into how historical temperature conditions impact the declared purposes of incomers.

This application comprises an Apache Airflow dag which dynamically spins up an AWS EMR + Hadoop cluster, it then uses the Spark steps API to cleanse, combine and aggregate I94 arrivals data, a set of demographic data by city, airport codes and a Kaggle dataset of state temperatures by month to generate and populate a simple star schema in Redshift. 

The star schema consists of a fact table containing anonymised arrivals visa type data, in addition to gender counts and average age information aggregated by month and state. It combines this with the most recent historical temperature data for that state.

A single dimension table, keyed by state code contains additional demographic data by state. Since this data is non time-variant and effectively a property of the state it is not stored in the fact table. 

Setup
--
1) Install Apache-Airflow (you will need 1.10.14 rather than 2.0.0), [so follow the steps here.](https://airflow.apache.org/docs/apache-airflow/1.10.14/installation.html)
2) Install and verify the[AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-mac.html) and ensure that you have set up a default set of access credentials.
3) Create an S3 bucket to contain the datasets and pyspark_steps scripts. The folder structure should be the same as the one below:
![enter image description here](https://i.ibb.co/4Pps09K/Screenshot-2021-02-19-at-21-31-17.png)
4) Open a terminal session & cd to your airflow home directory (usually ~/airflow) and type:

		git remote add -t \* -f origin https://github.com/felipeazucares/udacity_6.git
		git checkout capstone
	
	This should download all  of the source files into your airflow directory (do not forget the checkout step as this project is not on the master branch).
5) Upload the ~/airflow/additional_data/GlobalLandTemperaturesByState.csv file to the your s3://<your bucket>/data folder.
	The data folder should now look like this:
![enter image description here](https://i.ibb.co/VpQ0cgm/Screenshot-2021-02-19-at-21-32-45.png)
7) Open the ~/airflow/dags/capstone_emr_redshift.py dag in an editor and change this line:
	
		BUCKET_NAME  =  "capstone-suggars"
		
	To reflect the name that you have called your S3 bucket.
8) Now upload the contents of the ~/airflow/pyspark_steps directory to the pyspark_steps folder under your s3 bucket.
	
	The pyspark_steps s3 folder should now contain:
![enter image description here](https://i.ibb.co/pnj4FWy/Screenshot-2021-02-19-at-21-34-46.png)
9) Spin up a Redshift cluster. You should name your schema dev and make a note of:
	-  User
	- Password
	- Endpoint  
	- Port

10) From your terminal session start the airflow webserver and scheduler, then open a browser and go to http://localhost:<your port>/admin/ to view the Airflow UI.
11) Once the Airflow cache has refreshed you should see the capstone_emr_redshift dag listed in the dag bag 
12) In the Airflow UI go to Admin->Connections and add a new Postgres connection, name it 'redshift' and enter the details for the cluster you created in step 3 and click save.
13) From the main Airflow UI you should now be able to execute the capstone_emr_redshift dag by toggling it to 'On' and clicking on the trigger dag 'play' switch on the far right of the screen. Click the 'Trigger' button on the confirmation page.

The process should take around 40-60 minutes to run. Once complete you should be able to run the example queries included at the bottom of this write up on the resulting star schema.

Describe and Gather Data
--
As discussed in the previous section, the datasets utilised in this project are:
- i94 arrivals data - e.g. i94_apr16_sub.sas7bdat. There are 12 of these files in all, each in sas7bdat format and containing a month's worth of arrivals data at the level of anonymised individual. Each record contains the self declared, gender, age and visa type information for the traveller as well as port of arrival:
	*I94YR* - 4 digit year
	*I94MON* - Numeric month
	I94CIT & I94RES - Citizen and residency
	*I94PORT* - This format shows all the valid and invalid codes for processing
	ARRDATE is the Arrival Date in the USA. It is a SAS date numeric field that a permanent format has not been applied. Please apply whichever date format works for you.
	I94MODE - There are missing values as well as not reported (9)
	I94ADDR - There is lots of invalid codes in this variable and the list below shows what we have found to be valid, everything else goes into 'other'
	DEPDATE is the Departure Date from the USA. It is a SAS date numeric field that a permament format has not been applied. Please apply whichever date format works for you.
	*I94BIR* - Age of Respondent in Years
	*I94VISA* - Visa codes collapsed into three categories:
	COUNT - Used for summary statistics
	DTADFILE - Character Date Field - Date added to I-94 Files - CIC does not use
	VISAPOST - Department of State where where Visa was issued - CIC does not use
	OCCUP - Occupation that will be performed in U.S. - CIC does not use
	ENTDEPA - Arrival Flag - admitted or paroled into the U.S. - CIC does not use
	ENTDEPD - Departure Flag - Departed, lost I-94 or is deceased - CIC does not use
	ENTDEPU - Update Flag - Either apprehended, overstayed, adjusted to perm residence - CIC does not use
	MATFLAG - Match flag - Match of arrival and departure records
	BIRYEAR - 4 digit year of birth
	DTADDTO - Character Date Field - Date to which admitted to U.S. (allowed to stay until) - CIC does not use
	*GENDER* - Non-immigrant sex
	INSNUM - INS number
	AIRLINE - Airline used to arrive in U.S.
	ADMNUM - Admission Number
	FLTNO - Flight number of Airline used to arrive in U.S.
	VISATYPE - Class of admission legally admitting the non-immigrant to temporarily stay in U.S.
	
- World Temperature Data by State - GlobalLandTemperaturesByState.csv. This dataset comes from Kaggle and lists average monthly temperatures at the state level, up to and including 2013: 
	*dt* - datetime for temperature reading 
	*AverageTemperature* - average temperature for region
	AverageTemperatureUncertainty
	*State* - Region of reading
	*Country* - Country for region
	
-  U.S. City Demographic Data us-cities-demographics.csv  This dataset is sourced from OpenSoft. It contains demographic information by US city. The data is broken down by ethnic group, so each city has four separate records:
	*City*
	*State*
	*Median Age*
	*Male Population*
	*Female Population*
	*Total Population*
	Number of Veterans
	Foreign-born
	Average Household Size
	*State Code*
	Race
	Count
	
-   Airport Code Table - airport-code_csv.csv. This is a simple table of airport codes and corresponding cities from around the world sourced from datahub.io
	ident - airport identifier
	type - type of airport
	name
	elevation_ft
	continent
	iso_country
	*iso_region iso region code - for US this is a proxy for state*
	municipality
	gps_code
	iata_code 
	*local_code*
	coordinates	

Note:  fields in italics are those used in the construction of the fact and dimension tables.

The temperature data I opted  to use in this process is by state by month rather than by city by month. The reason for this is that summarising the city temperature data to state level seemed unlikley to give an accurate picture of average state temperatures given the unknown concentration of cities per state and the amount of rural data that would be left out of such an averaging process.

Since the state level dataset is readily available on Kaggle summarising the data to state seemed an artificial constraint that would lead to very inaccurate results.

Exploring, Assessing and Cleaning the Data
--
In assessing these datasets I have focused on completeness rather than other aspects of data quality as this is likely to have the most benefical impact on the results produced.

**Temperature Data**
The main issues with the temperature data were:
1) The latest data were for the year 2013, whereas the I94 arrivals data were for the year 2016. For the purposes of this exploration it seemed reasonable to filter this set to just 2013 and to combine this with the 2016 arrivals data, since climate change is a relatively incremental phenomenon and there were unlikely to be any marked differences year on year. 
2) The data for the months Sept-Dec were missing for the latest year, 2013. I therefore opted to extract these months for the previous three years (2010-2012) and average them out, then append these generated months back into the main dataset to provide complete coverage for all of the months for 2013.

**U.S. City Demographic Data**
The main issues with this dataset were:
1) The dataset provided four records for each city split by four race classes. Apart from the ethnicity specific fields the data in each of these records was the same. Therefore, I opted to drop any duplicates resulting in one record per city
2) The data was at city level, but my intention was to generate a schema summarised by month and state, therefore I needed to summarise to state level before using it to construct a dimension table. I did this by aggregating across the measures that I was interested in, to provide population counts and demographic splits across the state. However, I am very aware that this data only pertains to the urban population in the cities noted and  therefore significantly understates the total populations of these measures, a situation which I've addressed by suffixing these columns with 'urban population'.

**I94 Arrivals Data**
There were two major issues and assumptions that I had to make with this dataset:
1) As already identified the dataset was from 2016 whereas the temperature data was from 2013.
2) It seemed likely that the 194addr field contained the onward address of each arrival, however, this was not always completed and in some cases might not be accurate at all. Therefore, I opted to take the i94port field as this was likely to be more accurate and render a larger final dataset of valid arrivals. I then joined this with the local_code field on the airport_codes dataset to provide an arrival state code for each of the records in the data set. I could then aggregate the measures I was interested in to state month and year level.

**Airport code data**
The issues I detected with this dataset were:
1) Firstly the data would need to be restricted to US airports
2) While the iso-region field was very close to a state code - there were some iso-regions which did not map back to a state, I therefore needed to cleanse this data by joining it to my state table such that only airports in valid states remained.
3) There were several possible join candidates for the i94port field in the I94 dataset, namely: ident, iata_code and local_code. Some experimentation showed that local_code returned more hits than the other two, so I settled upon this as my key for joining the tables to build out the fact table.

Define the Data Model
--
The joining and cleansing steps taken are documented in the diagrams below:

**Conceptual flow**
![enter image description here](https://i.ibb.co/r0XN5Tp/I94-processing-flow-chart.png)
**Entity Relationship Diagram**
The schema is comprised of two tables, a fact table, fact_arrivals containing arrivals facts aggregated to month and state level and a single dimension table, dimension_state, containing non-variant US state properties. 
![enter image description here](https://i.ibb.co/tLF0sBR/I94-ERD.png)

**Data Dictionary**
The dimension_state table contains property information for the state dimension. 
*Dimension_state table*
Field | Storage | Definition
:----- | :---- | :-----
state_key   | varchar(2) - primary key| Standard 2 character state identifier
state_name  | varchar(256)  | Name of state 
average_age | float8| average age of urban population |
female_urban_population | bigint| population count of female state residents in cities included in dataset |
male_urban_population | bigint| population count of male state residents in cities included in dataset |
total_urban_population | bigint| total population count of state residents in cities included in dataset |

*Fact_arrivals table*
The fact_arrivals table contains demographic arrivals facts aggregated by state month and year. It contains a foreign key, state_key which references the primary key of the dimension_state table.

Field | Storage | Definition
:----- | :---- | :-----
state_key   | varchar(2) - foreign key, primary key component | Standard 2 character state identifier
month  | integer  | month of arrival 
year | integer| year of arrival |
average_age_arrivals | float8| average age of arrivals for state_key, month and year|
F | bigint| Count of arrivals who identify as female for state_key, month and year |
M | bigint| Count of arrivals who identify as male for state_key, month and year |
U | bigint| Count of arrivals who identify as non-binary for state_key, month and year
X | bigint| Count of arrivals who identify as trans for state_key, month and year |
business | bigint| Count of arrivals travelling for business for state_key, month and year |
pleasure | bigint| Count of arrivals travelling for pleasure for state_key, month and year |
student | bigint| Count of arrivals travelling for study for state_key, month and year |
average_temperature | float8| average temperature of state for month 


Toolset used
--
Since the application needed to process around 7Gb of data, I felt that a master/slave EMR cluster on AWS with Hadoop would provide the necessary compute and throughput capacity to process the datasets.

The resultant schema itself would best be stored in a columnar database such as Redshift. Apache Cassandra would not be flexible enough for the queries that I envisaged being required, at least, not without having to generate multiple tables to support all possible query combinations, which seemed like an overhead for this use case.

To manage the transformation and processing orchestration I used a locally installed instance of Apache-Airflow as its tight integration with both EMR and Redshift would make the process both reliable and relatively lightweight. Similarly, I was able to make use of the EMR's steps API to ensure the resulting dag was not overly complex or reliant on CLI calls.

Update cadence
--
Given that the data is summarised by month, it would probably make little sense to update the dataset more than a monthly cadence, although it should be stressed that the pipeline itself is perfectly able to cope with inter-month updates. Users would just need to be aware that the current month figures were effectively a work-in-progress.

Other scenarios
--
**Data x100**
If the datasets involved were increased in scale by 100% it would be necessary to increase the number of slave nodes in the EMR cluster significantly, similarly the airflow dag could be split into sub dags to enhance opportunities for parallelism. Currently, for example, all months are aggregated together and processed as one. It should be possible to load and process each of these separately in parallel. 

**Daily 7AM pipeline**
To run this pipeline daily at 7AM it would simply be necessary to update the capstone_emr_redshift dag's schedule interval.

**Database users 100+**
To provide access to 100+ people it would probably be necessary to increase the scale of the redshift clusters involved, although the exact configuration would depend upon the operations that these users were carrying out. 

At the very least, one would need to move to a more formal IAM structure with authorisation and authentication more rigidly stratified and managed. 

That said, it's entirely possible that with enough network and compute resources allocated the existing schema would support query only access for quite a significant user population if thought was given to the sort and partition keys.

Example queries
--
The example queries below have been run in the resulting Redshift schema to illustrate how it is possible to derive insight from the underlying datasets involved.  E.g. *If we want to see how the arrivals travel motivation is affected by average state temperature*

	select month,business,pleasure,student,average_temperature from fact_arrivals 
	join dimension_state on fact_arrivals.state_key = dimension_state.state_key
	where fact_arrivals.state_key = 'CA' order by month

Results:
![enter image description here](https://i.ibb.co/2sxqfJt/Screenshot-2021-02-19-at-21-10-07.png)

Here it is possible to see that there is a significant correlation between the number of travellers arriving "for pleasure" and the average state temperature, while the number of business arrival remains relatively flat.

*What is the relative gender mix of arrivals to California*:

	select month, F, M, U, X, average_temperature from fact_arrivals 
	join dimension_state on fact_arrivals.state_key = dimension_state.state_key
	where fact_arrivals.state_key = 'CA' order by month

Results:
![enter image description here](https://i.ibb.co/Mf7fxkZ/Screenshot-2021-02-19-at-21-16-55.png)

So we can see that although the summer months induce an increase in arrivals across genders, there are generally more female arrivals regardless of the temperature.



