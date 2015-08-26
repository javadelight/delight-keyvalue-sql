package de.mxro.async.map.sql;

public class SqlConfigurations {

    public static SqlConnectionConfiguration inMemoryH2() {
        return new SqlConnectionConfiguration() {
            
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
                return "MERGE INTO "+getTableName()+" (Id, Value) KEY (Id) VALUES (?, ?)";
            }

            override String getConnectionString() {
                return "jdbc:h2:mem:test"
            }

            override String getTableName() {
                return "test"
            }
        };
    }

}
