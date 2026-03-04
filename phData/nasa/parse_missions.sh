#!/usr/bin/bash 

### Date : 03-03-2026 
### Auth : roger hill 
### Desc : Get the first $1 nasa missions and parse them out into separate directories and json files 

# wget https://osdr.nasa.gov/geode-py/ws/api/missions
# mv missions all_missions.raw 
# sudo apt install jq
# cat all_missions.raw | jq > all_missions.json

MISSIONS_TO_DOWNLOAD="$1" 

for i in $(grep mission all_missions.json | awk '{print $2}'|head -$MISSIONS_TO_DOWNLOAD)
do  
  mission_name=$(echo $i|awk -F/ '{print $NF}'|sed 's/"//g')
  echo "Creating folder : \"mission/$mission_name\"" 
  mkdir "missions/$mission_name" 2>/dev/null 
  i=$(echo $i|sed 's/"//g')
  echo $i  
  wget "$i" -T 10 -O missions/$mission_name/mission_data.raw
  sleep 1 
  cat missions/$mission_name/mission_data.raw | jq > missions/$mission_name/mission_data.json 
  rm -rf missions/$mission_name/mission_data.raw 
  echo "-----------------------------------------------------------------------" 
done

exit 0 
