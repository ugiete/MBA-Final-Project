FROM public.ecr.aws/lambda/python:3.9

COPY app.py ${LAMBDA_TASK_ROOT}
COPY requirements.txt .

RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

ENV AWS_REGION=eu-central-1
ENV DATA_BUCKET=ugiete-hycom

CMD [ "app.main" ]