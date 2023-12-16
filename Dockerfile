FROM amazon/aws-lambda-python:3.9

COPY requirements.txt ${LAMBDA_TASK_ROOT}
COPY chainflip.py ${LAMBDA_TASK_ROOT}

RUN pip install -r requirements.txt 


# The command that gets run after the container is initiated
CMD [ "chainflip.lambda_handler" ]