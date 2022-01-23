#!/bin/bash

update_data_in_sh() {
	filename=$1
	classname=$2
  echo "#!/bin/bash" > $filename
  echo "./gradlew clean run -PmainClass="$classname >> $filename
}

script_folder_name=run_spark_applications_scripts
if [ -z "$1" ]
then
  overwrite=false
else
  overwrite=true
fi
echo $overwrite

if [ ! -d $script_folder_name ]
then
	mkdir $script_folder_name
fi


for file in `find . -type f -regex ".*\.scala"`
do
	filename=$(echo $file | awk -F "/" '{print $NF}' | cut -d "." -f1)
	path=$(echo $file | rev | cut -d "/" -f 2- | rev)
	packagename=$(echo $file | awk -F "/" '
		BEGIN { ORS="" }; 
			{for(i=5;i<NF-1;++i)print $i"."};
			{print $i}
		') 
	mainClassName=$(echo "$packagename.$filename")
	shellScriptName=$(echo $script_folder_name/$(echo $filename | cut -d "." -f1  \
		| sed 's/\([^A-Z]\)\([A-Z0-9]\)/\1_\2/g' \
		| sed 's/\([A-Z0-9]\)\([A-Z0-9]\)\([^A-Z]\)/\1_\2\3/g' \
		| tr '[:upper:]' '[:lower:]')".sh")

	if [[ $overwrite == "false" && -f $shellScriptName ]]; then
	  echo $shellScriptName skipped...
	else
    update_data_in_sh $shellScriptName $mainClassName
    echo $shellScriptName created...
    chmod 755 $shellScriptName
	fi
done;