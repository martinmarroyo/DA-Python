FROM python:3.9
# Set working directory
# TODO: Rethink directory structure
WORKDIR /src/python-env
# Set python path to import from src 
ENV PYTHONPATH=/src
# Set up environment
RUN apt update
RUN apt upgrade -y
RUN apt install nano -y
# Generate SSH key to link w/ Github
RUN ssh-keygen -t rsa -f ~/.ssh/git-key -q -N '""'
# Copy code files
# TODO: make sure ETL is in it's own folder
COPY etl /src
COPY requirements.txt /src
RUN pip install -r ../requirements.txt
# Set access token for Jupyter server
ENV JUPYTER_TOKEN=datalab
# TODO: Add command to start Jupyter on launch
# Keep our container running
CMD ["tail", "-f", "/dev/null"]