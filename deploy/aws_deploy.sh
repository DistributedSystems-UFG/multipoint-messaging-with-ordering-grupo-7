#!/usr/bin/env bash
# =============================================================================
# AWS multi-region deployment — Causal-Order multipoint messaging
#
# Topology (7 EC2 instances, Ubuntu 22.04 t3.micro):
#   naming-service  →  us-east-1      (N. Virginia)  — single static address
#   peer-1          →  us-east-1      (N. Virginia)
#   peer-2          →  us-west-2      (Oregon)
#   peer-3          →  eu-west-1      (Ireland)
#   peer-4          →  ap-southeast-1 (Singapore)
#   peer-5          →  sa-east-1      (São Paulo)
#   peer-6          →  ap-northeast-1 (Tokyo)
#
# Prerequisites
# -------------
#   - AWS CLI configured with credentials (aws configure)
#   - A key pair named "pp14-key" in EACH region used below
#     (or edit KEY_NAME per region)
#   - Docker Hub account; images already pushed:
#       docker build -t <HUB_USER>/pp14-naming -f naming_service/Dockerfile .
#       docker build -t <HUB_USER>/pp14-peer    -f peer/Dockerfile          .
#       docker push <HUB_USER>/pp14-naming
#       docker push <HUB_USER>/pp14-peer
#   - Adjust DOCKERHUB_USER, KEY_NAME, and AMI IDs for your account/regions.
#
# Usage
#   chmod +x deploy/aws_deploy.sh
#   ./deploy/aws_deploy.sh
# =============================================================================

set -euo pipefail

DOCKERHUB_USER="rafaelstaveira"
KEY_NAME="pp14-key"
INSTANCE_TYPE="t3.micro"
PORT_NAMING=50050
PORT_PEER=50070

# Ubuntu 22.04 LTS AMI IDs (update if needed — these expire)
declare -A AMI=(
  [us-east-1]="ami-0c7217cdde317cfec"
  [us-west-2]="ami-008fe2fc65df48dac"
  [eu-west-1]="ami-0905a3c97561e0b69"
  [ap-southeast-1]="ami-0fa377108253bf620"
  [sa-east-1]="ami-0af6e9042ea5a4e3e"
  [ap-northeast-1]="ami-0d52744d6551d851e"
)

# ── helpers ──────────────────────────────────────────────────────────────────

wait_for_instance() {
  local region=$1 instance_id=$2
  echo "  Waiting for $instance_id in $region to be running…"
  aws ec2 wait instance-running --instance-ids "$instance_id" --region "$region"
}

get_public_ip() {
  local region=$1 instance_id=$2
  aws ec2 describe-instances \
    --instance-ids "$instance_id" \
    --region "$region" \
    --query "Reservations[0].Instances[0].PublicIpAddress" \
    --output text
}

create_sg() {
  local region=$1 name=$2 port=$3
  local sg_id
  sg_id=$(aws ec2 create-security-group \
    --group-name "$name" \
    --description "pp14 $name" \
    --region "$region" \
    --query "GroupId" --output text 2>/dev/null || true)

  if [[ -z "$sg_id" ]]; then
    # Already exists — fetch its ID
    sg_id=$(aws ec2 describe-security-groups \
      --filters "Name=group-name,Values=$name" \
      --region "$region" \
      --query "SecurityGroups[0].GroupId" --output text)
  fi

  # Allow inbound on the service port and SSH
  aws ec2 authorize-security-group-ingress \
    --group-id "$sg_id" --region "$region" \
    --protocol tcp --port "$port" --cidr 0.0.0.0/0 2>/dev/null || true
  aws ec2 authorize-security-group-ingress \
    --group-id "$sg_id" --region "$region" \
    --protocol tcp --port 22 --cidr 0.0.0.0/0 2>/dev/null || true

  echo "$sg_id"
}

# User-data script that installs Docker and runs a container
naming_userdata() {
  local image="$DOCKERHUB_USER/pp14-naming"
  cat <<EOF
#!/bin/bash
apt-get update -y
apt-get install -y docker.io
systemctl start docker
docker pull $image
docker run -d --restart=always -p $PORT_NAMING:$PORT_NAMING \
  -e PORT=$PORT_NAMING \
  $image
EOF
}

peer_userdata() {
  local name=$1 ns_addr=$2 image="$DOCKERHUB_USER/pp14-peer"
  cat <<EOF
#!/bin/bash
apt-get update -y
apt-get install -y docker.io
systemctl start docker
# Wait until the public IP is available via metadata
PUBLIC_IP=\$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)
docker pull $image
docker run -d --restart=always -p $PORT_PEER:$PORT_PEER \
  -e PEER_NAME="$name" \
  -e PEER_HOST="\$PUBLIC_IP" \
  -e PEER_PORT=$PORT_PEER \
  -e NAME_SERVICE_ADDRESS="$ns_addr" \
  -e MSG_INTERVAL_LO=3.0 \
  -e MSG_INTERVAL_HI=7.0 \
  $image
EOF
}

# ── Step 1: naming service (us-east-1) ───────────────────────────────────────

echo "==> Launching naming-service in us-east-1"
NS_SG=$(create_sg us-east-1 "pp14-naming-sg" $PORT_NAMING)

NS_INSTANCE=$(aws ec2 run-instances \
  --region us-east-1 \
  --image-id "${AMI[us-east-1]}" \
  --instance-type "$INSTANCE_TYPE" \
  --key-name "$KEY_NAME" \
  --security-group-ids "$NS_SG" \
  --user-data "$(naming_userdata)" \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=pp14-naming}]' \
  --query "Instances[0].InstanceId" --output text)

echo "  Instance: $NS_INSTANCE"
wait_for_instance us-east-1 "$NS_INSTANCE"
NS_IP=$(get_public_ip us-east-1 "$NS_INSTANCE")
NS_ADDR="${NS_IP}:${PORT_NAMING}"
echo "  Naming service public IP: $NS_IP  (addr=$NS_ADDR)"

# ── Step 2: launch 6 peers in 6 different regions ────────────────────────────

declare -A PEER_REGIONS=(
  [peer-1]="us-east-1"
  [peer-2]="us-west-2"
  [peer-3]="eu-west-1"
  [peer-4]="ap-southeast-1"
  [peer-5]="sa-east-1"
  [peer-6]="ap-northeast-1"
)

declare -A PEER_INSTANCES=()

for peer in peer-1 peer-2 peer-3 peer-4 peer-5 peer-6; do
  region="${PEER_REGIONS[$peer]}"
  echo "==> Launching $peer in $region"

  peer_sg=$(create_sg "$region" "pp14-peer-sg" $PORT_PEER)

  iid=$(aws ec2 run-instances \
    --region "$region" \
    --image-id "${AMI[$region]}" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$peer_sg" \
    --user-data "$(peer_userdata "$peer" "$NS_ADDR")" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=pp14-$peer}]" \
    --query "Instances[0].InstanceId" --output text)

  echo "  Instance: $iid"
  PEER_INSTANCES[$peer]="$iid"
done

# ── Step 3: wait + report public IPs ─────────────────────────────────────────

echo ""
echo "==> Waiting for all peer instances to be running…"
for peer in "${!PEER_INSTANCES[@]}"; do
  region="${PEER_REGIONS[$peer]}"
  wait_for_instance "$region" "${PEER_INSTANCES[$peer]}"
  ip=$(get_public_ip "$region" "${PEER_INSTANCES[$peer]}")
  echo "  $peer  ($region)  →  $ip:$PORT_PEER"
done

echo ""
echo "==> Deployment complete!"
echo "    Naming service: $NS_ADDR"
echo ""
echo "    Allow ~60 s for Docker to install and peers to register."
echo "    Then tail logs on any instance:"
echo "      ssh -i <your-key.pem> ubuntu@<PEER_IP>"
echo "      docker logs -f \$(docker ps -q)"
echo ""
echo "    To watch all logs from your laptop (requires ssh access):"
echo "      for each peer IP, run: ssh ubuntu@IP 'docker logs -f \$(docker ps -q)'"
