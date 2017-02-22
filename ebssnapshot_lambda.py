import boto3
import collections
import time
from botocore.client import ClientError

# ec2オブジェクト
ec2 = boto3.client('ec2')

# main関数のようなもの
def lambda_handler(event, context):
    descriptions = create_snapshots()
    delete_old_snapshots(descriptions)

# スナップショット作成関数
def create_snapshots():
    instances = get_instances(['Backup-Generation'])

    descriptions = {}

    # Backup-Generation タグ付インスタンスを走査
    for i in instances:
        tags = { t['Key']: t['Value'] for t in i['Tags'] }
        generation = int( tags.get('Backup-Generation', 0) )

        if generation < 1:
            continue

        for b in i['BlockDeviceMappings']:
            if b.get('Ebs') is None:
                continue

            # スナップショットの Description 
            # EBS VolumeID と EC2 name
            # EC2インスタンス名がない場合は EBS VolumeID のみ
            volume_id = b['Ebs']['VolumeId']
            description = volume_id if tags.get('Name') is '' else '%s(%s)' % (volume_id, tags['Name'])
            description = 'Auto Snapshot ' + description

            # スナップショット作成
            snapshot = _create_snapshot(volume_id, description)
            print 'create snapshot %s(%s)' % (snapshot['SnapshotId'], description)

            descriptions[description] = generation

    return descriptions

# インスタンス情報取得関数(タグ付インスタンス情報を取得)
def get_instances(tag_names):
    reservations = ec2.describe_instances(
        Filters=[
            {
                'Name': 'tag-key',
                'Values': tag_names
            }
        ]
    )['Reservations']

    return sum(
        [
            [i for i in r['Instances']]
            for r in reservations
        ], [])

# 古いスナップショットの削除関数(保存件数を超えたスナップショットを削除)
def delete_old_snapshots(descriptions):
    snapshots_descriptions = get_snapshots_descriptions(descriptions.keys())

    for description, snapshots in snapshots_descriptions.items():
        delete_count = len(snapshots) - descriptions[description]

        if delete_count <= 0:
            continue

        # StartTime の昇順(古い順)でソートするラムダ式
        snapshots.sort(key=lambda x:x['StartTime'])

        old_snapshots = snapshots[0:delete_count]

        for s in old_snapshots:
            _delete_snapshot(s['SnapshotId'])
            print 'delete snapshot %s(%s)' % (s['SnapshotId'], s['Description']) 

# Description(キー) に対するスナップショット情報のディクショナリ作成関数
def get_snapshots_descriptions(descriptions):
    snapshots = ec2.describe_snapshots(
        Filters=[
            {
                'Name': 'description',
                'Values': descriptions,
            }
        ]
    )['Snapshots']

    groups = collections.defaultdict(lambda: [])
    { groups[ s['Description'] ].append(s) for s in snapshots }

    return groups

# スナップショット作成関数(2回 Try) : private
def _create_snapshot(id, description):
    for i in range(1, 3):
        try:
            return ec2.create_snapshot(VolumeId=id,Description=description)
        except ClientError as e:
            print str(e)
        time.sleep(1)
    raise Exception('cannot create snapshot ' + description)

# スナップショット削除関数(2回 Try) : private
def _delete_snapshot(id):
    for i in range(1, 3):
        try:
            return ec2.delete_snapshot(SnapshotId=id)
        except ClientError as e:
            print str(e)
        time.sleep(1)
    raise Exception('cannot delete snapshot ' + id)