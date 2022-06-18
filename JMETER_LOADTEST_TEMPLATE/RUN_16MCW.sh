# This is a sample load test that can run SQL against different warehouse configurations at the same time
# You can commment out the tests you don't want to run

START=./
#A=
B=TPC_XS0101_16MCW
C=TPC_SM0101_16MCW
#D=
#E=
END=.sh
UNDER='_'

# These are the names of the sh files for each test
#NAME1=$START$A$END
NAME2=$START$B$END
NAME3=$START$C$END
#NAME4=$START$D$END
#NAME5=$START$E$END


echo "START SNOWFLAKE TEST 16"

# Run scripts
# echo "START: "$NAME1
# sh $NAME1
echo "START: "$NAME2
sh $NAME2
echo "START: "$NAME3
sh $NAME3
#echo ""
#echo "START: "$NAME4
#sh $NAME4
#echo ""
#echo "START: "$NAME5
#sh $NAME5

echo "DONE!" 