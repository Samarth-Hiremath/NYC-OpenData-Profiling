# Data Profiling and Analysis of NYC OpenData

### Abstract
Data processing or analysis cannot happen without having knowledge about the quality and reliability of data. Data has to be reviewed and the structure needs to be understood before we proceed to analysis. This makes *Data Profiling* a crucial stage in the data lifecycle.  
In this project, we profiled 1900 datasets from [NYC OpenData](https://opendata.cityofnewyork.us/data/) for both generic and semantic properties. After profiling the data, we noticed that most of the columns are composed of heterogeneous data types and there are many columns with null values. Some basic analysis of the generic profiles of all the datasets gave us information about the data types which frequently co-occur in columns — Integer and Text data types occur together most frequently. With the data profiles available, we could clean the data and use it for further analysis.  
We considered the *311 Service Requests Dataset* to analyse certain borough-wise and agency-wise trends in complaints. We did find multiple data quality issues in the dataset, but were all resolved and cleaned. As quite expected, noise turned out to be the top complaint type in all the five boroughs and Brooklyn accounts for the highest count of 311 Service Requests. It is also interesting that NYPD has the best non-emergency complaint resolution time.

Read the [project report](./Project%20Report.pdf) for more information.

Code written to use [pyspark](https://pypi.org/project/pyspark/) package.

### Contributors
[Samarth S Hiremath](https://github.com/Samarth-Hiremath)  
[Tanya Sah](https://github.com/tsah20)  
[Nitin Patil](https://github.com/nitinz1994)