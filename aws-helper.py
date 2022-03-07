import boto3


class AWS(object):
    def __init__(self):
        query = AWS.send_query_athena()
        response = AWS.get_response_s3(query)
        print(AWS.get_result_from_athena(response=response, column_index=1))

    @staticmethod
    def client():
        client = boto3.client('athena')
        return client

    @staticmethod
    def send_query_athena():
        client = AWS.client()
        query_execution_id = client.start_query_execution(
            QueryString="SELECT * from users limit 100 order by desc",
            QueryExecutionContext={
                'Database': 'users'
            },
            ResultConfiguration={
                'OutputLocation': "s3://users/spacial_information/"

            }
        )
        return query_execution_id

    @staticmethod
    def get_response_s3(query):
        client = AWS.client()
        response = client.get_query_results(
            QueryExecutionId=query['QueryExecutionId'],
            MaxResults=10
        )
        return response

    @staticmethod
    def get_result_from_athena(response, column_index=1):
        return response['ResultSet']['Rows'][1]['Data'][column_index]['phone_number']
