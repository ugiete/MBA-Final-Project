FROM public.ecr.aws/lambda/python:3.9

# Must include app.py and requirements.txt files
COPY app.py ${LAMBDA_TASK_ROOT}
COPY requirements.txt .

RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

#ENV VAR=VALUE

# The function will call the main method
CMD [ "app.main" ]