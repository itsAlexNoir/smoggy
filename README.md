# smoggy
A code that implements a deep learning model for predicting air quality from open data.

### Start up the database server

We use MongoDB (a NoSQL database) for storing all the data. In order to start the server, run the script
´´´ bash bin/start_smoggydb.sh ´´´
By default, the database is located at localhost, port 27021. You can connect with the parameters via Mongo shell, or any GUI for Mongo available. We use the extension for [VS Code].

[VS Code] https://marketplace.visualstudio.com/items?itemName=mongodb.mongodb-vscode

#### Backups
You can back-up the database at any time using this command.
mongodump --host=<your_host> --port=<port_number> --out=<path to te backup> 

Also, if the database become corrupt (this has happended in the past), you can restore the databse from a given backup using this command:
mongorestore --host=<your_host> --host=<port number> <path_to_the_backup>