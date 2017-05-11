#!/usr/bin/env bash
echo "Creating Couches on Docker"
docker run -d --name couchdb -p 5984:5984 klaemo/couchdb:1.6.1
docker run -d --name couchdb2 -p 5985:5984 klaemo/couchdb:1.6.1
echo "Waiting for Couches to initialize..."
for ((i=1;i<=10;i++)); do
    sleep 1
    printf "."
done
printf "\n"
echo "Creating admins (users: test/test)"
curl -XPUT http://localhost:5984/_config/admidns/test -H "Content-type: application/json" --data '"test"'
curl -XPUT http://localhost:5985/_config/admidns/test -H "Content-type: application/json" --data '"test"'
