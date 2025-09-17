FROM dalmasbluesquarehub/templates-ci:latest

WORKDIR /app/openhexa_templates

# COPY ./requirements-dev.txt ./requirements-dev.txt
COPY ./templates_ci_requirements.txt ./templates_ci_requirements.txt

RUN python3.11 -m pip install --upgrade pip
RUN python3.11 -m pip install Cython==0.29.30
# RUN python3.11 -m pip install -r requirements-dev.txt
RUN python3.11 -m pip install -r templates_ci_requirements.txt

COPY ./ ./
