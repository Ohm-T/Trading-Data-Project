import torch
import torch.nn as nn

class LSTM(nn.Module):
    def __init__(self, hidden_size, input_size, num_layers, num_classes):
        super(LSTM, self).__init()
        self.num_classes = num_classes
        self.hidden_size = hidden_size
        self.input_size = input_size
        self.num_layers = num_layers
        self.lstm = LSTM(input_size=input_size,
                        hidden_size=hidden_size,
                        num_layers=num_layers,
                        batch_first=True)
        self.fc1 = nn.Linear(hidden_size, 128)
        self.fc2 = nn.Linear(128, num_classes)
        self.relu = nn.ReLU()

    def forward(self, x_in):
        out, (hn, cn) = self.lstm(x_in)
        hn = hn.view(-1, self.hidden_size)


