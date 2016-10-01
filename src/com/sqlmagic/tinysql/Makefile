#---------------------------------------------------------------
# Makefile
#
# $Author: $
# $Date: $
# $Revision: $
#
# make or make jar will both rebuild any out of date classes and
# create a new jar file in the directory "classes"
#
# If you add or rename a java file the normal make may fail because
# of inter-dependencies between the classes.  This can normally be
# corrected by using make recompile.  However, all classes will be
# updated in this situation.
#
# Revision History:
#
# Major rewite by Davis Swan in November 2004 so that only classes
# that need to be updated are recompiled.  
#
#---------------------------------------------------------------

INSTALL_CLASS_DIR = ../../../../classes
INSTALL_DIR = $(INSTALL_CLASS_DIR)/com/sqlmagic/tinySQL

JAVAC = javac -d $(INSTALL_CLASS_DIR) -classpath $(INSTALL_CLASS_DIR)

ROOT_DIR = ../../../../
JAR_DIR = $(ROOT_DIR)classes
CLASS_DIR = $(JAR_DIR)/com/sqlmagic/tinySQL

OBJECTS=$(CLASS_DIR)/Utils.class \
	$(CLASS_DIR)/tinySQLException.class \
	$(CLASS_DIR)/tinySQLGlobals.class \
	$(CLASS_DIR)/tinySQLTable.class \
	$(CLASS_DIR)/tinySQLWhere.class \
	$(CLASS_DIR)/UtilString.class \
	$(CLASS_DIR)/SimpleXMLTag.class \
	$(CLASS_DIR)/FieldTokenizer.class \
	$(CLASS_DIR)/tsColumn.class \
	$(CLASS_DIR)/dbfFileTable.class \
	$(CLASS_DIR)/DBFHeader.class \
	$(CLASS_DIR)/dbfFile.class \
	$(CLASS_DIR)/dbfFileConnection.class \
	$(CLASS_DIR)/dbfFileDatabaseMetaData.class \
	$(CLASS_DIR)/dbfFileDriver.class  \
	$(CLASS_DIR)/textFile.class \
	$(CLASS_DIR)/textFileConnection.class \
	$(CLASS_DIR)/textFileDriver.class \
	$(CLASS_DIR)/textFileTable.class \
	$(CLASS_DIR)/tinySQL.class \
	$(CLASS_DIR)/tinySQLCmd.class \
	$(CLASS_DIR)/tinySQLConnection.class \
	$(CLASS_DIR)/tinySQLDriver.class \
	$(CLASS_DIR)/tinySQLParser.class \
	$(CLASS_DIR)/tinySQLPreparedStatement.class \
	$(CLASS_DIR)/tinySQLResultSet.class \
	$(CLASS_DIR)/tinySQLResultSetMetaData.class \
	$(CLASS_DIR)/tinySQLStatement.class \
	$(CLASS_DIR)/tinySQLDatabaseMetaData.class \
	$(CLASS_DIR)/tsResultSet.class \
	$(CLASS_DIR)/tsRow.class

.SUFFIXES: .java .class

#
# The first target is the default made using the make command with no arguments
#
jar: $(OBJECTS)

	(cd $(INSTALL_CLASS_DIR);jar cvfm tinySQL.jar MANIFEST.MF com/sqlmagic/tinysql/*.class)
#	jar cvfm $(INSTALL_CLASS_DIR)/tinySQL.jar  $(INSTALL_CLASS_DIR)/MANIFEST.MF -C $(CLASS_DIR)/*.class
#
# The following rule can be used to make individual class files.  This is 
# useful for getting aroung dependency issues.
#

.java.class:
	$(JAVAC) $<
 
$(CLASS_DIR)/%.class: %.java
	$(JAVAC) $<
#
# Because of Java file dependencies, an incremental make will sometimes fail.
# In that case use make recompile to let the Java compiler resolve all 
# dependencies and make the new set of class files.
#

recompile:
	$(JAVAC) *.java

all: $(OBJECTS)
	make jar

clean: 
	rm -f *.class 
	rm -f $(INSTALL_DIR)/*.class 
	rm -f $(INSTALL_CLASS_DIR)/*.class
	rm -f $(INSTALL_CLASS_DIR)/tinySQL.jar

