package com.mishinyura.bookstesting.database;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.test.context.ActiveProfiles;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class SchemaTests {
    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbc;

    @DisplayName("Test DB Schema V1")
    @Test
    void shouldTestDBSchemaV1() throws Exception {
        Set<String> sysTables = Set.of();
        Set<String> tablesExpected = Set.of(
                "books"
        );
        int tablesQuantityExpected = 1;

        Set<String> tablesActual = new HashSet<>();
        try (ResultSet rs = Objects.requireNonNull(dataSource.getConnection().getMetaData()
                .getTables(null, null, null, new String[]{"TABLE"}))) {
            while (rs.next()) {
                tablesActual.add(rs.getString("TABLE_NAME"));
            }
        }
        tablesActual.removeAll(sysTables);

        assertThat(tablesActual.size()).isEqualTo(tablesQuantityExpected);
        assertThat(tablesActual).containsAll(tablesExpected);
    }

    @DisplayName("Test DB Schema V2")
    @Test
    void shouldTestDBSchemaV2() {
        Set<String> sysTables = Set.of();
        String sqlQuery = "SELECT * FROM information_schema.tables WHERE table_schema = 'public';";
        Set<String> tablesExpected = Set.of(
                "books"
        );
        int tablesQuantityExpected = 1;
        Set<String> tablesActual = new HashSet<>();

        SqlRowSet rs = jdbc.queryForRowSet(sqlQuery);
        while (rs.next()) {
            tablesActual.add(rs.getString("TABLE_NAME"));
        }
        tablesActual.removeAll(sysTables);

        assertThat(tablesActual.size()).isEqualTo(tablesQuantityExpected);
        assertThat(tablesActual).containsAll(tablesExpected);
    }
}
