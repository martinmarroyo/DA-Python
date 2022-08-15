FROM python:3.9
# Set working directory
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
COPY etl /src/etl
RUN pip install -r /src/etl/requirements.txt
# Set access token for Jupyter server & run it
ENV JUPYTER_TOKEN=datalab
# Start the Jupyter server
CMD ["jupyter-notebook","--ip","0.0.0.0", "--no-browser", "--allow-root"]
