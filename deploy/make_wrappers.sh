#!/bin/sh
# Create the commands pipeline, biosignal-mock and check_ground_truth.
#   sh deploy/make_wrappers.sh /opt/heartbeat
set -eu
PREFIX=${1:?install prefix, e.g. /opt/heartbeat}

wrapper() {  # wrapper NAME MODULE
    cat > "/usr/local/bin/$1" <<EOF
#!/bin/sh
PYTHONPATH=$PREFIX exec python3 -m $2 "\$@"
EOF
    chmod +x "/usr/local/bin/$1"
}
wrapper pipeline heartbeat
wrapper biosignal-mock mock.biosignal_mock
wrapper check_ground_truth mock.check_ground_truth
