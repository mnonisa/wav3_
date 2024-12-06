#! /bin/bash

ansible-playbook -i ansible/inventory_files/wave_inven ansible/playbooks/wav3_.yml --ask-become-pass
