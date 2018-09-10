package org.culpan.trebuchet.ui.controllers;

import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.MenuBar;

public class MainController {
    @FXML
    MenuBar mainMenuBar;

    @FXML
    public void initialize() {
        mainMenuBar.setUseSystemMenuBar(true);
        mainMenuBar.setVisible(false);
    }

    @FXML
    public void handleClose(ActionEvent actionEvent) {
        System.exit(0);
    }

    @FXML void handleOpen(ActionEvent actionEvent) {

    }
}
