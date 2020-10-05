# smoggy
A code that implements a deep learning model for predicting air quality from open data.

## Usage
### Start up the database server

We use MongoDB (the popular NoSQL database) for storing all the data. In order to start the server, run the script:
```bash bin/start_smoggydb.sh```

By default, the database is located at localhost, port 27021. You can connect with the parameters via Mongo shell, or any GUI for Mongo available. We recommend the extension for [VSCode](https://marketplace.visualstudio.com/items?itemName=mongodb.mongodb-vscode).

####  About backups
You can back-up the database at any time using this command (We strongly recommend this. You never know when fail happens):
```mongodump --host=<your_host> --port=<port_number> --out=<path to te backup> ```

Also, if the database become corrupt (yes, this has happended to us in the past), you can restore the database from a given backup using this command:
```mongorestore --host=<your_host> --host=<port number> <path_to_the_backup>```
