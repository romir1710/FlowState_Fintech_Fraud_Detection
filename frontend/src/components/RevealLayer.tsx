/**
 * RevealLayer.tsx — Cursor-following spotlight that reveals a second image
 *
 * Renders BG_IMAGE_2 masked behind a soft radial gradient circle that
 * tracks the user's mouse position. Uses a hidden <canvas> to generate
 * the alpha mask as a dataURL, then applies it as CSS maskImage on
 * the reveal <div>.
 */

import { useRef, useEffect, useCallback } from 'react';

interface RevealLayerProps {
  image: string;
  cursorX: number;
  cursorY: number;
  spotlightRadius: number;
}

export default function RevealLayer({ image, cursorX, cursorY, spotlightRadius }: RevealLayerProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const revealRef = useRef<HTMLDivElement>(null);

  const updateMask = useCallback(() => {
    const canvas = canvasRef.current;
    const reveal = revealRef.current;
    if (!canvas || !reveal) return;

    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    // Clear
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    // Build radial gradient at cursor position
    const gradient = ctx.createRadialGradient(
      cursorX, cursorY, 0,
      cursorX, cursorY, spotlightRadius,
    );
    gradient.addColorStop(0, 'rgba(255,255,255,1)');
    gradient.addColorStop(0.4, 'rgba(255,255,255,1)');
    gradient.addColorStop(0.6, 'rgba(255,255,255,0.75)');
    gradient.addColorStop(0.75, 'rgba(255,255,255,0.4)');
    gradient.addColorStop(0.88, 'rgba(255,255,255,0.12)');
    gradient.addColorStop(1, 'rgba(255,255,255,0)');

    ctx.fillStyle = gradient;
    ctx.beginPath();
    ctx.arc(cursorX, cursorY, spotlightRadius, 0, Math.PI * 2);
    ctx.fill();

    // Apply mask
    const dataUrl = canvas.toDataURL();
    reveal.style.maskImage = `url(${dataUrl})`;
    reveal.style.webkitMaskImage = `url(${dataUrl})`;
    reveal.style.maskSize = '100% 100%';
    reveal.style.webkitMaskSize = '100% 100%';
  }, [cursorX, cursorY, spotlightRadius]);

  // Size canvas to viewport
  useEffect(() => {
    const resize = () => {
      const canvas = canvasRef.current;
      if (!canvas) return;
      canvas.width = window.innerWidth;
      canvas.height = window.innerHeight;
    };
    resize();
    window.addEventListener('resize', resize);
    return () => window.removeEventListener('resize', resize);
  }, []);

  // Update mask on every cursor change
  useEffect(() => {
    updateMask();
  }, [updateMask]);

  return (
    <>
      {/* Hidden canvas — generates the alpha mask */}
      <canvas
        ref={canvasRef}
        className="absolute inset-0 pointer-events-none"
        style={{ display: 'none' }}
      />
      {/* Reveal div — shows BG_IMAGE_2 through the mask */}
      <div
        ref={revealRef}
        className="absolute inset-0 bg-center bg-cover bg-no-repeat z-30 pointer-events-none"
        style={{ backgroundImage: `url(${image})` }}
      />
    </>
  );
}
