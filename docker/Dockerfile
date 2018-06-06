FROM kappa:deps

WORKDIR /root

ENV PATH="$PATH:/root:/root/compiler"
ENV KAPPA_PATH="/root"

COPY examples/ examples/
COPY deps/compiler/invoker.py deps/compiler/do_transform.py compiler/
COPY deps/compiler/rt/ compiler/rt/
COPY deps/compiler/transform/ compiler/transform/
COPY coordinator .
COPY setup .
COPY run .

EXPOSE 43731:43731
