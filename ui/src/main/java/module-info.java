module org.culpan.trebuchet.ui {
    requires org.culpan.hdb;
    requires javafx.graphics;
    requires javafx.fxml;
    requires javafx.controls;

    exports org.culpan.trebuchet.ui to javafx.graphics;
    exports org.culpan.trebuchet.ui.controllers to javafx.fxml;
    opens org.culpan.trebuchet.ui.controllers to javafx.fxml;
}