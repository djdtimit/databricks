# Docker Image for Development Environments

This `Dockerfile` creates a Docker image with the following contents:

## Prerequisites

Before using a VS Code devcontainer, you need to have the following installed on your machine:

1. [Docker](https://www.docker.com/) or [Rancher Desktop](https://rancherdesktop.io/)
  ,Docker Desktop requires a license. Rancher Desktop does not require a license.
  When using Rancher Desktop make sure to run the container engine with `dorckerd (moby)`.
  This can be set in the preferences when starting the application.

1. [Visual Studio Code](https://code.visualstudio.com/)
  The Software is free to use and no license is required.

1. In VSCode select the Extensions Tab on the left-hand side and install the
  [VS Code Remote - Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension.

### Using the DevContainer

1. Docker or Rancher Desktop application needs to be running.

1. Open VSCode.

1. If you already checked out the repo, open the local repository and select a branch where the `.devcontainer`
  folder exists in the repo. If not, decide whether you want to check out the repo locally or check out
  the repo in a container volume.

    If you want to check out the repository in a container volume chose the first option in the next step.

1. Press `Ctrl + Shift + P` (on Windows) and type either:
    - `Dev Containers: Clone Repository in Container Volume`. Select the correct GitHub Project that you want
       to check out. Select the desired branch (devcontainer definition needs to exist on the branch).
    - `Dev Containers: Open Folder in Container`, if you already cloned the Code Repository locally and
       don't want to use a Container Volume.
1. Once the devcontainer is started, you can access the command-line interface (CLI) by opening the VS Code terminal
  (`View > Terminal`) and running commands as usual.

1. Start developing!

## Included Software

- Ubuntu 22.04 LTS
- Additional OS packages:
  - `python3`: This is the version 3 implementation of the Python programming language.
  - `python3-dev`: This is the development package for Python 3, which includes header files and other resources
    needed for building Python modules.
  - `python3-pip`: This is the package manager for Python 3, which allows you to install and manage Python packages
    (collections of Python modules).
  - `gnupg`: This is the GNU Privacy Guard, a free software implementation of the OpenPGP standard
    for encrypting and signing data.
  - `software-properties-common`: This is a package that provides an interface for managing the software sources
    that are used for package installation on a Debian-based system.
  - `build-essential`: This is a package that includes a set of tools necessary for building software from source code.
  - `libssl-dev`: This is the development package for the OpenSSL library, which provides support for secure networking
    (e.g. SSL/TLS) in Python and other applications.
  - `libffi-dev`: This is the development package for the Foreign Function Interface library, which allows Python
     and other languages to call functions written in other languages.
- Software Installations
  - `requests`: This is a Python package that allows you to send HTTP requests using Python.
  - `terraform`: This is a tool for building, changing, and versioning infrastructure safely and efficiently.
  - `terraform-docs`: This is a tool for generating documentation for Terraform modules.
  - `az cli`: This is the command-line interface for Azure, a cloud computing platform and infrastructure created by Microsoft.
  - `jq`: This is a command-line tool for processing JSON data.
  - `powershell`: This the PowerShell command-line shell and scripting language in version `7.3.0`.
  - `shellcheck`: This is a static analysis tool for shell scripts, which checks for problems in shell scripts
     and provides suggestions for how to fix them.
  - `pyenv`: [Pyenv](https://github.com/pyenv/pyenv.git) is a tool for managing and installing different versions of python.
  - `openjdk-11-jre-headless`: Openjdk Java JRE 11 for local execution of Spark applications. Headless version
     as no GUI application are run.

  - Additional packages from `requirements.txt`:
    - `build`: This package contains tools for building Python projects.
    - `databricks-cli`: This package provides a command-line interface for interacting with Databricks workspaces and clusters.
    - `kapitan`: This package provides tools for managing and organizing large and complex configurations.
    - `msal`: This package provides the Microsoft Authentication Library (MSAL), which allows Python applications
      to authenticate with Azure AD and other Microsoft services.
    - `msrest`: This package provides the msrest namespace, which contains classes and methods for working with
      Azure REST APIs in Python.
    - `typing-extensions`: This package provides additional type hints for Python.
    - `pipenv`: [Pipenv](https://pipenv.pypa.io/en/latest/) is a virtual environment and package manager for
      Python projects for an easy setup of a development environment.

  - Additional python packages maintaned via [Pipenv](../Pipfile) and Datafoundation library dependencies (see [setup.cfg](../library/datafoundation/setup.cfg)):
    - `mypy`: This package provides static type checking for Python, specifically version 0.991 of the mypy package.
    - `pydocstyle`: This package provides support for checking the style of docstrings in Python, specifically
      version 6.1.1 of the pydocstyle package.
    - `pylint`: This package provides a code linter for Python, specifically version 2.15.8 of the pylint package.
    - `pytest`: This package provides tools for testing Python code, specifically version 7.2.0 of the pytest package.
    - `flake8`: This package provides a code linter for Python, specifically version 6.0.0 of the flake8 package.
    - `pyspark`: This package provides an interface for Apache Spark in Python for developing Spark applications.
    - `delta-spark`: This package provides the Pythone APIs for using Delta Lake with Apache Spark.
    - `azure-core`: This package provides the core functionality for building Azure integrations in Python applications.
    - `azure-data-tables`: This package provides the azure.data.tables namespace, which contains classes and methods
      for working with Azure Table storage in Python.
    - `azure-identity`: This package provides classes and methods for authenticating with Azure services in Python applications.
    - `azure-keyvault-secrets`: This package provides the azure.keyvault.secrets namespace, which contains classes
      and methods for working with secrets in Azure Key Vault in Python.
    - `azure-storage-file-datalake`: This package provides the azure.storage.file.datalake namespace, which contains
      classes and methods for working with Azure Data Lake storage in Python.
    - `python-dateutil`: This package provides support for working with dates and times in Python, specifically version
      2.8.2 of the python-dateutil package.
    - `pypi-json`: This package provides a JSON API for working with JSON data in Python.

This image is intended to be used as a base for development environments in Visual Studio Code's "devcontainers" feature.
It includes tools for working with Terraform, Azure, and Python.

## Important Versions

Versions for

- `terraform`
- `terraform-docs`
- `powershell`

can be set als Build-Arguments. Additional a Python version is set. This should be the Python version currently used in
the Databricks Runtime version running on the clusters in the Data Foundation. An overview over which versions are used
by Databricks is given in the [runtime release notes](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/releases).

## DevContainer Description

devcontainer.json is a configuration file for a development container in Visual Studio Code.
It specifies the name of the container, the Dockerfile to use for building the container, commands to run after
starting the container, the ports that the container exposes, extensions to install in the container
and settings for the container.

In this specific configuration, the container is named "ORPDF Dev Container" and uses a Dockerfile called "Dockerfile"
for building the container. After starting the container, a virtual environment is created and the pip command is run
to install the library/datafoundation package in editable mode. No post-create command is specified, and a list of
extensions is provided to be installed in the container. The settings specify that the container uses python as the
Python interpreter, enables linting with pylint, pydocstyle, flake8, and mypy, and uses the black formatter with a
line length of 120 characters.

There is a "postStartCommand" field that specifies commands to run after starting the container,
which includes creating a virtual environment and installing the library/datafoundation package in editable mode.

### Extensions for VSCode

The "extensions" field in the devcontainer.json file specifies a list of extensions to be installed in the
development container. The list includes the following extensions:

- `davidanson.vscode-markdownlint`: This extension provides linting for Markdown files in Visual Studio Code.
- `eamodio.gitlens`: This extension provides advanced Git features in Visual Studio Code, such as code review,
  repository navigation, and code-aware navigation.
- `github.vscode-pull-request-github`: This extension allows users to create and review pull requests directly in
  Visual Studio Code.
- `gruntfuggly.todo-tree`: This extension allows users to view and manage their TODO comments in a tree-style view in
  Visual Studio Code.
- `hashicorp.terraform`: This extension provides syntax highlighting, validation, and IntelliSense for Terraform files
  in Visual Studio Code.
- `ms-python.black-formatter`: This extension provides formatting for Python files using the black formatter
  in Visual Studio Code.
- `ms-python.flake8`: is an extension for Visual Studio Code that provides linting for Python files using the flake8 package.
- `ms-python.pylint`: This extension provides linting for Python files using pylint in Visual Studio Code.
- `ms-python.python`: This extension provides support for the Python language in Visual Studio Code, including features
  such as IntelliSense, debugging, and code formatting.
- `ms-vscode.PowerShell`: This extension provides support for the PowerShell language in Visual Studio Code, including
  features such as IntelliSense and debugging.
- `ms-vscode.azure-account`: This extension allows users to sign in to Azure and manage their Azure subscriptions and
  resources directly in Visual Studio Code.
- `ms-vscode.azurecli`: This extension provides a command-line interface for Azure in Visual Studio Code, allowing users
  to run Azure CLI commands from within the editor.
- `ms-vsliveshare.vsliveshare`: This extension allows users to collaborate on code with others in real time
  in Visual Studio Code.
- `redhat.vscode-yaml`: This extension provides syntax highlighting and validation for YAML files in Visual Studio Code.

## More Information

For more information on using VS Code devcontainers, see the [VS Code documentation](https://code.visualstudio.com/docs/remote/containers).

As the container setup changes the ownership of the repository directory Git will complain about
`detected dubious ownership in repository`.
This is resolved while building the container through trusting the directory as a safe one in the global Git config.

### Library Development

The datafoundation library is installed in a virtual environment (venv). All environments and packages are managed via
[Pipenv](https://pipenv.pypa.io/en/latest/). An environment running on the Python version used by Databricks is
available when starting the container.

The datafoundation library is installed in editable mode, meaning code changes are available direclty,
e.g. when running tests.

__Dependency issues/missing packages popup when using the container the first time or after a rebuild:__

It can happen that VS Code complains about missing packages (e.g. pylint) when starting the container the first time.
Make sure that the environment created by Pipenv is selected as the Python interpreter. This can be done via
`Ctrl + Shift + P` and typing `Python: Select Interpreter` (or by opening a *.py file and selecting in the bottom right
of the VS Code GUI). The selected interpreter should have the Workspace folder name
(normally the repository name _orp-datafoundation-monorepo_) in its name.

After selecting the interpreter it may be necessay to __restart VS Code__, afterwards all packages and paths should be
discovery correctly (no linting errors for missing imports).

## Pipenv development

See also offical [basic usage](https://pipenv-fork.readthedocs.io/en/latest/basics.html) and
[advanced usage](https://pipenv.pypa.io/en/latest/advanced/) documentation.

### Activate environment

VS Code may do this autmatically already:

```bash
# pipenv shell
```

### Deactivate environemnt

```bash
# deactivate
```

### Running commands

It is possible to run commands even when the environment is not activated:

```bash
# pipenv run <command>
# pipenv run pytest .
# pipenv run pylint .
```

### Addtional packages

Install additional packages into the environment:

```bash
# pipenv install <package>
```

When adding a new dependency in setup.cfg run afterwards:

```bash
# pipenv install --dev
```

### Build wheel

To build the package and get an executable Wheel run

```bash
# python -m build --wheel
```

in the activated environment (else use `pipenv run python -m build --wheel`) while being in the library directory
(`library/datafoundation/`). The Wheel file will be created into the `dist/` directory.

### Testing

Within the activated environment and directory `library/datafoundation/` run

```bash
# pytest .
```

which will run all defined tests in `/test`.
