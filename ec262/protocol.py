import asynchat
import pickle
import logging

class Protocol(asynchat.async_chat):
    def __init__(self, conn=None):
        if conn:
            asynchat.async_chat.__init__(self, conn)
        else:
            asynchat.async_chat.__init__(self)
        self.set_terminator("\n")
        self._buffer = ""
        self._mid_command = None
        self._commands = {}
        self.register_command('disconnect', lambda x, y: self.handle_close())

    def collect_incoming_data(self, data):
        """Receive data and append it to an internal buffer"""
        self._buffer += data

    def send_command(self, command, data=None):
        """Send command and optional data over connection
        
        Colons and newlines are special characters and should not appear in 
        `command`. If `data` is specified, then it will be pickled and sent
        after `command`. The message sent is either `COMMAND:\\n` or
        `COMMAND:DATALENGTH\\nPICKELED_DATA`
        """
        command += ':'
        if data:
            pdata = pickle.dumps(data)
            self.push(command + str(len(pdata)) + '\n' + pdata)
        else:
            self.push(command + "\n")

    def found_terminator(self):
        """Process a received command
        
        If a full command has been received, pass it to `process_command`;
        otherwise set `self.mid_command` and await the amount of data
        specified by the 
        """
        if not self._mid_command:
            command, data_length = self._buffer.split(":", 1)
            if data_length:
                self.set_terminator(int(data_length))
                self._mid_command = command
            else:
                self.process_command(command)
        else:
            data = pickle.loads(self._buffer)
            self.set_terminator("\n")
            command = self._mid_command
            self._mid_command = None
            self.process_command(command, data)
        self._buffer = ""

    def register_command(self, command, handler):
        """Register a handler for the given command"""
        self._commands[command] = handler

    def process_command(self, command, data=None):
        """Call the method corresponding to the received command"""
        if command in self._commands:
            self._commands[command](command, data)
        else:
            logging.critical("Unknown command received: %s" % (command,)) 
            self.handle_close()
    
    def handle_close(self):
        """Handler for when connection should close"""
        self.close()