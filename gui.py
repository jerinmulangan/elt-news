from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QPushButton, QScrollArea,
    QListWidget, QListWidgetItem, QGroupBox
)
from PyQt5.QtCore import QTimer, Qt

class NewsApp(QWidget):
    def __init__(self, fetch_news_callback):
        super().__init__()
        self.setWindowTitle("News Aggregator")
        self.resize(800, 1000)
        self.fetch_news_callback = fetch_news_callback

        self.selected_platforms = ["CNN", "NBC", "NPR", "TLDR"]
        self.selected_topics = ["World", "Tech"]

        self.layout = QVBoxLayout()
        self.setLayout(self.layout)

        platform_group = QGroupBox("Select News Platforms")
        platform_layout = QVBoxLayout()
        self.platform_selector = QListWidget()
        self.platform_selector.setSelectionMode(QListWidget.MultiSelection)
        for platform in ["CNN", "NBC", "NPR", "TLDR"]:
            item = QListWidgetItem(platform)
            item.setSelected(platform in self.selected_platforms)
            self.platform_selector.addItem(item)
        platform_layout.addWidget(self.platform_selector)
        platform_group.setLayout(platform_layout)
        self.layout.addWidget(platform_group)

        topic_group = QGroupBox("Select News Topics")
        topic_layout = QVBoxLayout()
        self.topic_selector = QListWidget()
        self.topic_selector.setSelectionMode(QListWidget.MultiSelection)
        for topic in ["World", "US News", "Politics", "Business", "Sports", "Tech", "Infosec", "WebDev", "DevOps", "AI", "Data"]:
            item = QListWidgetItem(topic)
            item.setSelected(topic in self.selected_topics)
            self.topic_selector.addItem(item)
        topic_layout.addWidget(self.topic_selector)
        topic_group.setLayout(topic_layout)
        self.layout.addWidget(topic_group)

        self.scroll = QScrollArea()
        self.inner_widget = QWidget()
        self.inner_layout = QVBoxLayout()
        self.inner_widget.setLayout(self.inner_layout)
        self.scroll.setWidget(self.inner_widget)
        self.scroll.setWidgetResizable(True)
        self.layout.addWidget(self.scroll)

        refresh_button = QPushButton("Refresh News")
        refresh_button.setStyleSheet("padding: 10px; font-weight: bold;")
        refresh_button.clicked.connect(self.refresh_news)
        self.layout.addWidget(refresh_button)

        self.timer = QTimer()
        self.timer.timeout.connect(self.refresh_news)
        self.timer.start(30 * 60 * 1000)

        self.refresh_news()

    def get_selected_platforms(self):
        return [item.text() for item in self.platform_selector.selectedItems()]

    def get_selected_topics(self):
        return [item.text() for item in self.topic_selector.selectedItems()]

    def refresh_news(self):
        self.selected_platforms = self.get_selected_platforms()
        self.selected_topics = self.get_selected_topics()
        stories = self.fetch_news_callback(self.selected_platforms, self.selected_topics)

        for i in reversed(range(self.inner_layout.count())):
            self.inner_layout.itemAt(i).widget().setParent(None)

        for source, title, link in stories:
            lbl = QLabel(f"<b>[{source}]</b> <a href='{link}'>{title}</a>")
            lbl.setOpenExternalLinks(True)
            lbl.setWordWrap(True)
            lbl.setTextInteractionFlags(Qt.TextBrowserInteraction)
            self.inner_layout.addWidget(lbl)
