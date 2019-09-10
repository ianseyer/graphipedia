sudo docker stop graphipedia ;
sudo docker rm graphipedia ;
sudo docker run --detach --name graphipedia --restart always --volume ~/:/work -d graphipedia ;
sudo docker build -t graphipedia -f Dockerfile . ;
sudo docker logs -f graphipedia ;
wc -l ~/links-out.xml ;