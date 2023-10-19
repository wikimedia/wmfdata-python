import subprocess

from wmfdata.utils import check_kerberos_auth

def put(local_path, hdfs_path):
    """
    Puts the local file located at `local_path` into our Hadoop file system
    at `hdfs_path`. `hdfs_path` does not need to include the scheme ("hdfs://");
    relative paths are interpreted relative to your user directory.
    """
    check_kerberos_auth()
    subprocess.run(
        ["hdfs", "dfs", "-put", local_path, hdfs_path],
        check=True
    )

def delete(hdfs_path):
    """
    Deletes the single file in our Hadoop file system located at `hdfs_path`.
    `hdfs_path` does not need to include the scheme ("hdfs://"); relative paths
    are interpreted relative to your user directory.

    Deleting directories or multiple files is not supported.
    """
    check_kerberos_auth()
    subprocess.run(
        ["hdfs", "dfs", "-rm", hdfs_path],
        check=True
    )
