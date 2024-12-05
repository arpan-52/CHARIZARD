<p align="center">
    <img src="https://github.com/user-attachments/assets/f98b6f76-dfa2-48be-99d8-5cab74cf92ea" alt="Drawing" height="120" width="18%"/>
    <img src="https://github.com/user-attachments/assets/0e2a7069-3a5f-4012-9933-163961285621" alt="Drawing" height="120" width="18%"/>
    <img src="https://github.com/user-attachments/assets/49998009-7719-4f0a-8695-3c45f101067b" alt="Drawing" height="120" width="18%"/>
    <img src="https://github.com/user-attachments/assets/3dbe7340-7136-4076-a16e-76fd2fa21f40" alt="Drawing" height="120" width="18%"/>
    <img src="https://github.com/user-attachments/assets/97d29653-828d-4f9b-abea-ffdca500b771" alt="Drawing" height="120" width="18%"/>
</p>

This a highly automated radio interferometric imaging routine, which process the whole band data by treating each spw independently. 

**The uGMRT has a convention flip, the code assumes that the convention is taken care of. For reference, look at P.Chandra et al. 2023. If you want to change the convention after the data have been recorded then change the Stokes keywords by doing browsetable inside CASA and then changing the Stokes keywords from 5, 6, 7, 8 to 8, 7, 6, 5. This should take care of the flip in the convention.**

