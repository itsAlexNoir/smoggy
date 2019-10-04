# Start mongo daemon for aire db
# Currently this database is located at a external hard disk.

MONGO_CONFIG=/Volumes/TRIPLET/db/mongod_smoggydb.conf

echo "Starting mongo dameon for aire database"
mongod --config ${MONGO_CONFIG}
