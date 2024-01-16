rm -rf prerequisites
git clone https://github.com/Tracardi/com-tracardi -b 0.8.2-dev prerequisites

docker build ../ --rm --no-cache --progress=plain -f Dockerfile -t tracardi/apm:0.8.2-rc5
docker push tracardi/apm:0.8.2-rc5

rm -rf prerequisites