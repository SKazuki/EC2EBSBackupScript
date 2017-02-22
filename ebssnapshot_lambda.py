import boto3
import collections
import time
from botocore.client import ClientError

# ec2�I�u�W�F�N�g
ec2 = boto3.client('ec2')

# main�֐��̂悤�Ȃ���
def lambda_handler(event, context):
    descriptions = create_snapshots()
    delete_old_snapshots(descriptions)

# �X�i�b�v�V���b�g�쐬�֐�
def create_snapshots():
    instances = get_instances(['Backup-Generation'])

    descriptions = {}

    # Backup-Generation �^�O�t�C���X�^���X�𑖍�
    for i in instances:
        tags = { t['Key']: t['Value'] for t in i['Tags'] }
        generation = int( tags.get('Backup-Generation', 0) )

        if generation < 1:
            continue

        for b in i['BlockDeviceMappings']:
            if b.get('Ebs') is None:
                continue

            # �X�i�b�v�V���b�g�� Description 
            # EBS VolumeID �� EC2 name
            # EC2�C���X�^���X�����Ȃ��ꍇ�� EBS VolumeID �̂�
            volume_id = b['Ebs']['VolumeId']
            description = volume_id if tags.get('Name') is '' else '%s(%s)' % (volume_id, tags['Name'])
            description = 'Auto Snapshot ' + description

            # �X�i�b�v�V���b�g�쐬
            snapshot = _create_snapshot(volume_id, description)
            print 'create snapshot %s(%s)' % (snapshot['SnapshotId'], description)

            descriptions[description] = generation

    return descriptions

# �C���X�^���X���擾�֐�(�^�O�t�C���X�^���X�����擾)
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

# �Â��X�i�b�v�V���b�g�̍폜�֐�(�ۑ������𒴂����X�i�b�v�V���b�g���폜)
def delete_old_snapshots(descriptions):
    snapshots_descriptions = get_snapshots_descriptions(descriptions.keys())

    for description, snapshots in snapshots_descriptions.items():
        delete_count = len(snapshots) - descriptions[description]

        if delete_count <= 0:
            continue

        # StartTime �̏���(�Â���)�Ń\�[�g���郉���_��
        snapshots.sort(key=lambda x:x['StartTime'])

        old_snapshots = snapshots[0:delete_count]

        for s in old_snapshots:
            _delete_snapshot(s['SnapshotId'])
            print 'delete snapshot %s(%s)' % (s['SnapshotId'], s['Description']) 

# Description(�L�[) �ɑ΂���X�i�b�v�V���b�g���̃f�B�N�V���i���쐬�֐�
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

# �X�i�b�v�V���b�g�쐬�֐�(2�� Try) : private
def _create_snapshot(id, description):
    for i in range(1, 3):
        try:
            return ec2.create_snapshot(VolumeId=id,Description=description)
        except ClientError as e:
            print str(e)
        time.sleep(1)
    raise Exception('cannot create snapshot ' + description)

# �X�i�b�v�V���b�g�폜�֐�(2�� Try) : private
def _delete_snapshot(id):
    for i in range(1, 3):
        try:
            return ec2.delete_snapshot(SnapshotId=id)
        except ClientError as e:
            print str(e)
        time.sleep(1)
    raise Exception('cannot delete snapshot ' + id)