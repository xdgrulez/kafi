#!/bin/bash
echo "Scanning local directory..."
trivy fs --scanners vuln .
echo "...done."
#
echo "Building Docker image..."
sudo docker build --no-cache -t trivy_kafi .
echo "...done."
#
echo "Scanning Docker image..."
sudo trivy image --scanners vuln trivy_kafi
echo "...done."
#
echo "Cleaning up Docker image..."
sudo docker rmi trivy_kafi:latest
sudo docker rmi `awk '/^[Ff][Rr][Oo][Mm]/ {print $2; exit}' Dockerfile`
echo "...done."
