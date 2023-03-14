# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.245.2/containers/ubuntu/.devcontainer/base.Dockerfile

# [Choice] Ubuntu version (use ubuntu-22.04 or ubuntu-18.04 on local arm64/Apple Silicon): ubuntu (latest LTS), ubuntu-22.04, ubuntu-20.04, ubuntu-18.04
FROM mcr.microsoft.com/vscode/devcontainers/base:ubuntu-22.04

ARG TF_VERSION
ARG TF_DOCS_VERSION
ARG PS_VERSION
# Databricks Runtime Python Version
ARG DB_PY_VERSION  

# Install additional OS packages
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt update && apt install --no-install-recommends -y \
    python3 python3-dev python3-pip gnupg software-properties-common \
    build-essential libssl-dev libffi-dev

# Install terraform
RUN curl --fail --remote-time --location --output archive.zip https://releases.hashicorp.com/terraform/${TF_VERSION}/terraform_${TF_VERSION}_linux_$(dpkg --print-architecture).zip && \
    unzip -q archive.zip && \
    chmod +x terraform && \
    mv terraform /usr/local/bin/ && \
    terraform -v

# Install terraform-docs
RUN curl -Lo ./terraform-docs.tar.gz https://github.com/terraform-docs/terraform-docs/releases/download/v${TF_DOCS_VERSION}/terraform-docs-v${TF_DOCS_VERSION}-linux-$(dpkg --print-architecture).tar.gz \
    && tar -xzf terraform-docs.tar.gz \
    && chmod +x terraform-docs \
    && mv terraform-docs /usr/local/terraform-docs \
    && rm terraform-docs.tar.gz

# Install jq tool
RUN apt -y install jq shellcheck

# Install the powershell package
RUN if [ "$(dpkg --print-architecture)" = "arm64" ]; then \
    wget -q https://github.com/PowerShell/PowerShell/releases/download/v${PS_VERSION}/powershell-${PS_VERSION}-linux-$(dpkg --print-architecture).tar.gz && \
    mkdir -p /opt/microsoft/powershell/ && \
    tar zxf powershell-${PS_VERSION}-linux-$(dpkg --print-architecture).tar.gz -C /opt/microsoft/powershell/ && \
    chmod +x /opt/microsoft/powershell/pwsh && \
    ln -s /opt/microsoft/powershell/pwsh /usr/bin/pwsh; \
    else \
    wget -q https://github.com/PowerShell/PowerShell/releases/download/v${PS_VERSION}/powershell_${PS_VERSION}-1.deb_$(dpkg --print-architecture).deb && \
    dpkg -i powershell_${PS_VERSION}-1.deb_amd64.deb && \
    apt install -f; \
    fi

# Install python packages from requirements.txt
ADD requirements.txt .
RUN pip install -r requirements.txt
RUN pip install -U requests azure-cli

# Clean up and reduce the size of the Docker image by removing unnecessary files and packages.
RUN apt autoremove -y \
    && apt clean -y \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /tmp/build/*

# Install Java JRE, required for local Spark execution.
# As Spark 2.2 is used Java 11 is installed (18 not yet supported, 17 not working without errors).
RUN apt update && apt install -y openjdk-11-jre-headless

# Switch to the non-root user
USER vscode

# Install pyenv to be able to install and manage different versions of python.
# Also install version currenty used by Databricks for the DataFoundation.
RUN git clone https://github.com/pyenv/pyenv.git ~/.pyenv
RUN echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
RUN echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
RUN echo 'eval "$(pyenv init -)"' >> ~/.bashrc
RUN ~/.pyenv/bin/pyenv install -v ${DB_PY_VERSION}

# Enable git autocomplete
RUN echo "source /usr/share/bash-completion/completions/git" >> ~/.bashrc

# Mark repository path as a safe directory for git. Else error message in VS Code will popup and same setting will be
# applied.
RUN git config --global --add safe.directory /workspaces/orp-datafoundation-monorepo

# Switch back to dialog for any ad-hoc use of apt-get
ENV DEBIAN_FRONTEND=
