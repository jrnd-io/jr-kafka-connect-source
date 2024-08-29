FROM registry.access.redhat.com/ubi8/ubi as builder

ENV GO_VERSION=1.23.0
ENV GOPATH=/go
ENV PATH=$GOPATH/bin:/usr/local/go/bin:$PATH
ARG VERSION=0.4.0
ARG GOVERSION=$(go version)
ARG USER=$(id -u -n)
ARG TIME=$(date)

RUN dnf -y update && dnf -y install \
    gcc gcc-c++ make  \
    wget \
    tar \
    unzip \
    && dnf clean all

RUN wget https://golang.org/dl/go${GO_VERSION}.linux-amd64.tar.gz \
    && tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz \
    && rm go${GO_VERSION}.linux-amd64.tar.gz

RUN mkdir -p $GOPATH/src $GOPATH/bin

RUN wget -O jr.zip https://github.com/jrnd-io/jr/archive/refs/heads/main.zip
RUN unzip jr.zip -d /tmp
RUN rm jr.zip

WORKDIR /tmp/jr-main
COPY . .

RUN go install github.com/actgardner/gogen-avro/v10/cmd/...@latest
RUN go generate pkg/generator/generate.go
RUN go get -u -d -v
RUN CGO_ENABLED=1 GOOS=linux go build -tags static_all -v -ldflags="-X 'github.com/jrnd-io/jr/pkg/cmd.Version=${VERSION}' -X 'github.com/jrnd-io/jr/pkg/cmd.GoVersion=${GOVERSION}' -X 'github.com/jrnd-io/jr/pkg/cmd.BuildUser=${USER}' -X 'github.com/jrnd-io/jr/pkg/cmd.BuildTime=${TIME}'" -o build/jr jr.go

FROM confluentinc/cp-kafka-connect-base:7.7.0

COPY --from=builder /tmp/jr-main/templates/ /home/appuser/.jr/templates/
COPY --from=builder /tmp/jr-main/build/jr /bin

ENV JR_SYSTEM_DIR=/home/appuser/.jr

COPY target/jr-kafka-connect-source-0.0.1-package.zip /tmp/jr-kafka-connect-source-0.0.1-package.zip

RUN confluent-hub install --no-prompt /tmp/jr-kafka-connect-source-0.0.1-package.zip
