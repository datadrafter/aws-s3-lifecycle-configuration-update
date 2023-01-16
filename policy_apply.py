import boto3
from botocore.exceptions import ClientError

s3cli = boto3.client('s3')


''' Constants used in configuration '''

DAYS_AFTER_INITIATION = 29

multipart_rule = {
    'ID': 'AbortIncompleteMultipartUpload',
    'Prefix': '',
    'Status': 'Enabled',
    'AbortIncompleteMultipartUpload': {
        'DaysAfterInitiation': DAYS_AFTER_INITIATION
    }
}


''' Get bucket names as a list '''

buckets = s3cli.list_buckets()['Buckets']
bucket_names = list(map(lambda b: b['Name'], buckets))


''' Update buckets '''

for bucket_name in bucket_names:

    print('\nGetting lifecycle config of bucket "{0}"'.format(bucket_name))

    try:
        bucket_config = s3cli.get_bucket_lifecycle_configuration(Bucket=bucket_name)
    except ClientError:
        # bucket does not have lifecycle configuration
        bucket_config = {'Rules': []}

    bucket_lc_rules = bucket_config['Rules']
    multipart_rules = list(filter(lambda r: 'AbortIncompleteMultipartUpload' in r, bucket_lc_rules))

    # check whether rule is already applied to update and enable
    if len(multipart_rules) > 0:
        print('\tAlready has lifecycle rule - updating')
        for rule in multipart_rules:
            # update existing rule
            rule['Status'] = 'Enabled'
            rule['AbortIncompleteMultipartUpload']['DaysAfterInitiation'] = DAYS_AFTER_INITIATION
    else:
        # insert rule to the list
        print('\tDoes not have lifecycle rule - appending')
        bucket_lc_rules.append(multipart_rule)

    print('\tSaving changes')
    s3cli.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration={'Rules': bucket_lc_rules})



print('\nDone\n')