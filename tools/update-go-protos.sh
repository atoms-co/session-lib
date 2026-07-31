#!/usr/bin/env bash

# Updates Go files generated from protobufs using Bazel

set -euo pipefail

cd "$(dirname "$0")"/..

# Location
bazel build //proto/atoms/lib/net/session:session_go_proto
cp bazel-bin/proto/atoms/lib/net/session/session_go_proto_/go.atoms.co/lib/net/session/pb/*.go go/session/pb
chmod +w go/session/pb/*
