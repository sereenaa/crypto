FROM amazon/aws-lambda-python:3.9

COPY requirements.txt ${LAMBDA_TASK_ROOT}
COPY get_secret.py ${LAMBDA_TASK_ROOT}
COPY util.py ${LAMBDA_TASK_ROOT}
COPY validators.py ${LAMBDA_TASK_ROOT}
COPY circulation.py ${LAMBDA_TASK_ROOT}
COPY main.py ${LAMBDA_TASK_ROOT}


RUN pip install -r requirements.txt 


# The command that gets run after the container is initiated
CMD [ "main.lambda_handler" ]