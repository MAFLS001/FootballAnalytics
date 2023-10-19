import subprocess, getpass, shutil, os.path
# Retrieve User Name
u = str(getpass.getuser())
file = str('C:/Users/' + u + '/Downloads/cfp.sql')
f = 'C:/Users/' + u + '/cfp.sql'
# Move .sql File
if os.path.isfile(file):
    shutil.move(file, f)
# Execute .bat File
subprocess.call('MySQLImport.bat')