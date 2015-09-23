package de.mxro.async.map.sql;

public class SqlStoreConfigurations {

    public static abstract class MySQL extends SqlStoreConnectionConfiguration {
    
        @Override
        public boolean supportsInsertOrUpdate() {
            return true;
        }
    
        @Override
        public String getDriverClassName() {
            return "com.mysql.jdbc.Driver";
        }
    
        @Override
        public String getInsertOrUpdateTemplate() {
            return "INSERT INTO `" + getTableName()
                    + "`(`Id`, `Value`) VALUES (?,?) ON DUPLICATE KEY UPDATE `Value` = ?";
        }
    
        @Override
        public String getDeleteTableTemplate(final String tableName) {
            return "DROP TABLE `" + tableName + "`";
        }
    
        @Override
        public String getCreateTableTemplate(final String tableName) {
    
            return "CREATE TABLE `" + tableName
                    + "` (`Id` varchar( 999 ) CHARACTER SET ascii NOT NULL, `Value` longblob NOT NULL, PRIMARY KEY ( `Id` )) "
                    + "ENGINE = MYISAM DEFAULT CHARSET = latin1;";
        }
    
        @Override
        public boolean supportsMerge() {
            return false;
        }
    
    }

    public static abstract class Derby extends SqlStoreConnectionConfiguration {
    
        @Override
        public String getDriverClassName() {
            return "org.apache.derby.jdbc.EmbeddedDriver";
        }
    
        @Override
        public boolean supportsInsertOrUpdate() {
            return false;
        }
    
        @Override
        public boolean supportsMerge() {
            return false;
        }
    }

    public static abstract class H2 extends SqlStoreConnectionConfiguration {
    
        @Override
        public String getDriverClassName() {
            return "org.h2.Driver";
        }
    
        @Override
        public boolean supportsInsertOrUpdate() {
            return false;
        }
    
        @Override
        public boolean supportsMerge() {
            return true;
        }
    
        @Override
        public String getMergeTemplate() {
            return "MERGE INTO " + getTableName() + " (Id, Value) KEY (Id) VALUES (?, ?)";
        }
    }

    public static SqlStoreConnectionConfiguration inMemoryH2() {
        return new SqlStoreConnectionConfiguration() {

            @Override
            public String getDriverClassName() {
                return "org.h2.Driver";
            }

            @Override
            public boolean supportsInsertOrUpdate() {
                return false;
            }

            @Override
            public boolean supportsMerge() {
                return true;
            }

            @Override
            public String getMergeTemplate() {
                return "MERGE INTO " + getTableName() + " (Id, Value) KEY (Id) VALUES (?, ?)";
            }

            @Override
            public String getConnectionString() {
                return "jdbc:h2:mem:test";
            }

            @Override
            public String getTableName() {
                return "test";
            }
        };
    }

}
