#!/usr/bin/env bash
rm -rf .build

mkdir -p .build/deps
mkdir .build/examples

echo "Building coordinator"
CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o .build/coordinator ../coordinator/cmd/coordinator/

echo "Copying files over"
cp -r ../compiler .build/deps/compiler
cp -r ../examples/factorial .build/examples/factorial
cp ../requirements.txt .build/deps
cp setup .build/setup
cp run .build/run

cp deps/Dockerfile .build/deps
cp Dockerfile .build

docker build -t kappa:deps .build/deps
docker build -t kappa:latest .build

rm -r .build
