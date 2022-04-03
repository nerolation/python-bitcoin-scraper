# Copyright (C) 2015-2016 The bitcoin-blockchain-parser developers
#
# This file is part of bitcoin-blockchain-parser.
#
# It is subject to the license terms in the LICENSE file found in the top-level
# directory of this distribution.
#
# No part of bitcoin-blockchain-parser, including this file, may be copied,
# modified, propagated, or distributed except according to the terms contained
# in the LICENSE file.
#
# The last 2 `elif` and the `else ` statement in the `address` function were added to manage unknown 
# addresses. This is needed to ensure that the same number of addresses and values 
# are collected. 

from .utils import decode_varint, decode_uint64
from .script import Script
from .address import Address, UnknownAddress, OPReturnAddress


class Output(object):
    """Represents a Transaction output"""

    def __init__(self, raw_hex):
        self._value = None
        self._script = None
        self._addresses = None

        script_length, varint_size = decode_varint(raw_hex[8:])
        script_start = 8 + varint_size

        self._script_hex = raw_hex[script_start:script_start+script_length]
        self.size = script_start + script_length
        self._value_hex = raw_hex[:8]

    @classmethod
    def from_hex(cls, hex_):
        return cls(hex_)

    def __repr__(self):
        return "Output(satoshis=%d)" % self.value

    @property
    def value(self):
        """Returns the value of the output expressed in satoshis"""
        if self._value is None:
            self._value = decode_uint64(self._value_hex)
        return self._value

    @property
    def script(self):
        """Returns the output's script as a Script object"""
        if self._script is None:
            self._script = Script.from_hex(self._script_hex)
        return self._script
    

    @property
    def addresses(self):
        """Returns a list containing all the addresses mentioned
        in the output's script
        """
        if self._addresses is None:
            self._addresses = []
            if self.type == "p2pk":
                address = Address.from_public_key(self.script.operations[0])
                self._addresses.append(address)
            elif self.type == "p2pkh":
                address = Address.from_ripemd160(self.script.operations[2])
                self._addresses.append(address)
            elif self.type == "p2sh":
                address = Address.from_ripemd160(self.script.operations[1],
                                                 type="p2sh")
                self._addresses.append(address)
            elif self.type == "p2ms":
                n = self.script.operations[-2]
                for operation in self.script.operations[1:1+n]:
                    self._addresses.append(Address.from_public_key(operation))
                    # Break to only take one of the multisig addresses
                    break
            elif self.type == "p2wpkh":
                address = Address.from_bech32(self.script.operations[1], 0)
                self._addresses.append(address)
            elif self.type == "p2wsh":
                address = Address.from_bech32(self.script.operations[1], 0)
                self._addresses.append(address)
            elif self.type == "p2tr":
                address = Address.from_bech32m(self.script.operations[1], 1)
                self._addresses.append(address)
            elif self.type in ["OP_RETURN"]:
                opreturnaddress = OPReturnAddress(self.type)
                self._addresses.append(opreturnaddress)
            elif self.type in ["invalid", "unknown"]:
                unknownAddress = UnknownAddress(self.type)
                self._addresses.append(unknownAddress)
            else:
                unknownAddress = UnknownAddress("undefined")
                self._addresses.append(unknownAddress)

        return self._addresses

    def is_return(self):
        return self.script.is_return()

    def is_p2sh(self):
        return self.script.is_p2sh()

    def is_pubkey(self):
        return self.script.is_pubkey()

    def is_pubkeyhash(self):
        return self.script.is_pubkeyhash()

    def is_multisig(self):
        return self.script.is_multisig()

    def is_unknown(self):
        return self.script.is_unknown()

    def is_p2wpkh(self):
        return self.script.is_p2wpkh()

    def is_p2wsh(self):
        return self.script.is_p2wsh()

    def is_p2tr(self):
        return self.script.is_p2tr()

    @property
    def type(self):
        """Returns the output's script type as a string"""
        # Fix for issue 11
        if not self.script.script.is_valid():
            return "invalid"

        if self.is_pubkeyhash():
            return "p2pkh"

        if self.is_pubkey():
            return "p2pk"

        if self.is_p2sh():
            return "p2sh"

        if self.is_multisig():
            return "p2ms"

        if self.is_return():
            return "OP_RETURN"

        if self.is_p2wpkh():
            return "p2wpkh"

        if self.is_p2wsh():
            return "p2wsh"

        if self.is_p2tr():
            return "p2tr"

        return "unknown"
