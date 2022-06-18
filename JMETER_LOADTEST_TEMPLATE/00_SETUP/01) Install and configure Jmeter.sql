-------------
-- Steps to Install JMeter
-- Chris.Richardson@Snowflake.com
---------------

-- First you need to install JMeter
brew install jmeter

-- Update your bash profile so it starts in the root with terminal  
vim ~/.bash_profile
# i to Insert
# Add this line: alias jmeter='/usr/local/bin/jmeter'

-- Then reload the profile 
source ~/.bash_profile

-- You should now be able to check that it works
command -v jmeter

-- You can open the JMeter now with this command in terminal 
jmeter 

-- If you want to use JMeter with Snowflake or any other database you'll need to put the JDBC .jar file here:
/usr/local/opt/jmeter/libexec/lib