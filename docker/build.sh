rm -rf prerequisites
git clone https://github.com/Tracardi/com-tracardi -b 0.8.2 prerequisites

docker build ../ --rm --no-cache --progress=plain -f Dockerfile -t tracardi/apm:0.8.2.1
docker push tracardi/apm:0.8.2.1

rm -rf prerequisites