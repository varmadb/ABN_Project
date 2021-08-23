## INTRODUCTION:

Requirement:
 	A very small company called **KommatiPara** that deals with bitcoin trading has two separate datasets dealing with clients that they want to collate to starting interfacing more with their clients. One dataset contains information about the clients and the other one contains information about their financial details.

- Avoid using notebooks, like **Jupyter** for instance. While these are good for interactive work and/or prototyping in this case they shouldn't be used.
- Only use clients from the **United Kingdom** or the **Netherlands**.
- Remove personal identifiable information from the first dataset, **excluding emails**.
- Remove credit card number from the second dataset.
- Data should be joined using the **id** field.
- Rename the columns for the easier readability to the business users:

|Old name|New name|
|--|--|
|id|client_identifier|
|btc_a|bitcoin_address|
|cc_t|credit_card_type|

This application considers the following action 

- It will filter the data based on list of counties provide by user
- It will remove personal identifiable information from the client dataset and it will consider emails
- Remove credit card number from financial data set

Prerequisite:
-	User have to follow the pass the data set with below list of columns
Data set:
      o	Name: dataset_one.csv
      o	Columns: id,first_name,last_name,email,country
      o	Name: dataset_two.csv
      o	Columns: id,btc_a,cc_t,cc_n

-	Pull the project from github and follow the below steps for virtual env creation 
      o	pip install virtualenv
      o	virtualenv abnambro
      o	pip install -r requirements.txt
      o	pip install pipenv
      o	pipenv intall
 -	We need import below packages
      o	pip install chispa
      o	pip install pytest=2.9.1
      o	pip install pyspark

-	commands in console 
o	python Application.py Source_Dir/dataset_one.csv Source_Dir/dataset_two.csv "United Kingdom,Netherlands"



