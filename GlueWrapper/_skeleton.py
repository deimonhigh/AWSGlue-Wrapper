# Glue Default Import...
from GlueWrapper import gluewrapper # Import gluewrapper


@gluewrapper.run_etl(procedure="PROCEDURE_NAME", file_name="FILE_NAME")
def main():
	pass


main()
