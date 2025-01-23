import React from 'react';
import XLogo from '../../assets/pictures/x_logo.png';
import IGLogo from '../../assets/pictures/instagram_logo.png';
import './Footer.css'

export const Footer = () => {

  return (
    <div className="footer">
        <div className='social-media-container'>
          <a href='https://x.com/' target="_blank"><img src={XLogo} alt='x'/></a>
          <a href='https://instagram.com/' target="_blank"><img src={IGLogo} alt='ig'/></a>
        </div>
    </div>
  );
};

export default Footer;